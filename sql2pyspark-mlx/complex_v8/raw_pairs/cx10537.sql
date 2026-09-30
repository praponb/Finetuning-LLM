WITH unioned AS (
    -- Stack the branches
    SELECT id AS qty, adjuster_name AS name, level
    FROM adjusters
    WHERE level >= 1
    UNION ALL
    SELECT level, adjuster_name, id
    FROM adjusters
)
SELECT name, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM unioned
GROUP BY name
ORDER BY name
