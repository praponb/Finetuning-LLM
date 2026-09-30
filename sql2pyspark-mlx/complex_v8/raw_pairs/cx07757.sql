WITH unioned AS (
    -- Stack the branches
    SELECT course_id, score AS amount
    FROM enrollments
    UNION ALL
    SELECT id, gpa
    FROM students
)
SELECT course_id, COUNT(*) AS n_rows, SUM(amount) AS total_amount
FROM unioned
GROUP BY course_id
ORDER BY course_id
