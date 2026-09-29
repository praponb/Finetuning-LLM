WITH unioned AS (
    -- Stack the branches
    SELECT course_id AS qty, score AS amount, 'enrollment' AS origin
    FROM enrollments
    WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
    UNION ALL
    SELECT id AS qty, gpa AS amount, 'student' AS origin
    FROM students
)
SELECT origin, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM unioned
GROUP BY origin
ORDER BY origin
