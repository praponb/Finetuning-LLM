WITH unioned AS (
    -- Stack the branches
    SELECT id, credits AS key_id
    FROM courses
    UNION ALL
    SELECT id, credits AS key_id
    FROM courses
)
SELECT id, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM unioned
GROUP BY id
ORDER BY id
