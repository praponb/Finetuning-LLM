WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref,
        ROUND(score * 1.1, 2) AS score_adj
    FROM enrollments
    WHERE student_id >= 5
      AND score <= 73.0
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM filtered_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.student_id = s.student_id
              AND e.course_id >= 11
              AND e.enrolled_at >= TIMESTAMP '2025-02-06 11:00:00'
        )
)
SELECT *
FROM (
    SELECT course_id, student_id, grade_status, score_adj
    FROM with_activity
) src
PIVOT (
    AVG(score_adj) AS avg_v, COUNT(score_adj) AS n
    FOR grade_status IN ('FAIL', 'INCOMPLETE')
)
