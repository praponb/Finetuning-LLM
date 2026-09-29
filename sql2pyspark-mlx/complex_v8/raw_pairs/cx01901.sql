WITH stacked AS (
    -- Stack the branches
    SELECT id, level AS category
    FROM courses
    UNION ALL
    SELECT student_id AS id, term AS category
    FROM enrollments
    WHERE score > 16.3
      AND term <> '2025T2'
    UNION ALL
    SELECT course_id AS id, term AS category
    FROM enrollments
)
SELECT category, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM stacked
GROUP BY category
ORDER BY category
