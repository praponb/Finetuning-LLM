WITH combined AS (
    -- Stack the branches
    SELECT id AS ref_id, student_id AS key_id, score AS metric
    FROM enrollments
    UNION
    SELECT course_id, id, score
    FROM enrollments
    WHERE student_id IS NOT NULL
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM combined
GROUP BY ref_id
ORDER BY ref_id
