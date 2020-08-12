import rx
import rx.operators as ops
from time import sleep


NUM_TESTRUNS_PER_CATEGORY = 3

source = rx.timer(0, 7).pipe(
    ops.map(lambda i: i + 1),
    ops.take(2),
)

testresultStream = source.pipe(
    ops.flat_map_latest(
        lambda selectedJobId: getCategoryAndDependencies(selectedJobId)
    ),
)


def getCategoryAndDependencies(selectedJobId):
    return rx.of(selectedJobId).pipe(
        ops.flat_map(
            lambda selectedJobId: getCategory(selectedJobId)
        ),
        ops.flat_map(
            lambda category: getTestruns(category)
        ),
        ops.flat_map(
            lambda testruns: rx.from_(testruns)
        ),
        ops.map(
            lambda testrun: getTestrunDependencies(testrun)
        ),
        ops.merge(max_concurrent=1),
    )


def getCategory(selectedJobId):
    print('getting category...', selectedJobId)
    category = {
        'id': selectedJobId,
        'path': 'root/a/b/c',
    }
    return rx.of(category).pipe(ops.delay(1))


def getTestruns(category):
    print('getting testruns...', category)
    baseTestrunId = category['id'] * 10
    testruns = []
    for i in range(NUM_TESTRUNS_PER_CATEGORY):
        testrun = {
            'id': baseTestrunId + i,
            'name': 'testrun-{}'.format(baseTestrunId + i),
        }
        testruns.append(testrun)
    return rx.of(testruns).pipe(ops.delay(1))


def getTestrunDependencies(testrun):
    return rx.of(testrun).pipe(
        ops.flat_map(
            lambda testrun: getTestrunProperties(testrun)
        ),
        ops.flat_map(
            lambda testrun: getTestcaseOwners(testrun)
        ),
        ops.flat_map(
            lambda testrun: getTestrunMatrix(testrun)
        ),
    )


def getTestrunProperties(testrun):
    print('getting testrun properties...', testrun)
    properties = {
      'git_commit': '<git-hash>',
    }
    testrun['properties'] = properties;
    return rx.of(testrun).pipe(ops.delay(1));


def getTestcaseOwners(testrun):
    print('getting testcase owners...', testrun);
    owners = {
        'testcase': 'a.b.c',
        'owner': 'Sam Hawkins',
    }
    testrun['owners'] = owners
    return rx.of(testrun).pipe(ops.delay(1));


def getTestrunMatrix(testrun):
    print('getting testrun matrix...', testrun);
    matrix = {
        'testsuite': 'a.b',
        'testcases': [
            'c1',
            'c2',
            'c3',
            'c4',
        ],
    }
    testrun['matrix'] = matrix
    return rx.of(testrun).pipe(ops.delay(1));


setDone = False
def onCompleted():
    print("Done!")
    global setDone
    setDone = True


testresultStream.subscribe(
    #on_next = lambda i: print("Received '{0}'".format(i)),
    on_error = lambda e: print("Error Occurred: '{0}'".format(e)),
    on_completed = onCompleted,
)


try:
    while not setDone:
        sleep(.1)
except KeyboardInterrupt:
    print("\nTerminating...")
