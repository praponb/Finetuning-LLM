WITH all_events AS (
    -- Stack the branches
    SELECT id, score AS metric
    FROM enrollments
    UNION ALL
    SELECT student_id, score
    FROM enrollments
    WHERE score BETWEEN 5.9 AND 78.8
)
SELECT id, COUNT(*) AS n_rows, SUM(metric) AS total_metric
FROM all_events
GROUP BY id
ORDER BY id
