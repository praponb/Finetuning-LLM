WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ABS(score - 39.4) AS score_distance,
        enrolled_at + INTERVAL 30 MINUTES AS enrolled_at_plus_30m
    FROM enrollments
    WHERE course_id IS NOT NULL
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status NOT IN ('PASS', 'WITHDRAWN')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count
    FROM valid s
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.grade_status NOT IN ('FAIL', 'WITHDRAWN')
              AND TO_DATE(e.enrolled_at) < DATE '2025-02-06'
        )
)
SELECT
    id,
    student_id,
    enrolled_at_plus_30m,
    enrollment_count,
    enrollment_grade_status_count
FROM with_activity
WHERE grade_status <> 'WITHDRAWN'
ORDER BY enrolled_at_plus_30m, student_id DESC, id
LIMIT 20
