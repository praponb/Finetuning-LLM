WITH scoped AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        ROUND(score * 0.25, 2) AS score_adj
    FROM enrollments
    WHERE score >= 38.1
      AND term = '2024T2'
)
SELECT course_id, enrolled_at, 'enrollment' AS record_type
FROM scoped
WHERE grade_status = 'PASS'
UNION
SELECT id AS course_id, enrolled_at, 'enrollment_2' AS record_type
FROM enrollments
ORDER BY course_id
