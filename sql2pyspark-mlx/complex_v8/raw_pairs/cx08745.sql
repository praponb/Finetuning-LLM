WITH prepared AS (
    -- Scope the enrollments rows
    SELECT *
    FROM enrollments
    WHERE term NOT IN ('2025T1', '2025T2')
      AND grade_status IN ('WITHDRAWN', 'FAIL', 'INCOMPLETE')
),
enrollment_stats AS (
    -- Per-course enrollments metrics
    SELECT
        course_id,
        COUNT(CASE WHEN grade_status IN ('PASS', 'INCOMPLETE') THEN 1 END) AS enrollment_grade_status_count
    FROM enrollments
    GROUP BY course_id
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        COALESCE(e.enrollment_grade_status_count, 0) AS enrollment_grade_status_count
    FROM prepared s
    LEFT JOIN enrollment_stats e ON e.course_id = s.course_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_id, grade_status, score
    FROM profiled
) src
PIVOT (
    MIN(score)
    FOR grade_status IN ('PASS', 'WITHDRAWN', 'INCOMPLETE', 'FAIL')
)
ORDER BY course_id
