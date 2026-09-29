WITH combined AS (
    -- Stack the branches
    SELECT course_id AS entity_id, score, 'enrollment' AS origin
    FROM enrollments
    UNION ALL
    SELECT course_id AS entity_id, score, 'enrollment_2' AS origin
    FROM enrollments
)
SELECT origin, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM combined
GROUP BY origin
ORDER BY origin
