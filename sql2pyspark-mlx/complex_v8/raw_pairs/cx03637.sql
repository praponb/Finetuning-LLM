WITH prepared_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        TO_DATE(enrolled_at) AS enrolled_at_date
    FROM enrollments
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM prepared_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND TO_DATE(e.enrolled_at) < DATE '2025-03-08'
              AND e.grade_status = 'PASS'
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT enrolled_at, student_id, grade_status, score
    FROM screened
) src
PIVOT (
    SUM(score)
    FOR grade_status IN ('WITHDRAWN', 'INCOMPLETE', 'PASS', 'FAIL')
)
