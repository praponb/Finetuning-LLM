WITH all_events AS (
    -- Stack the branches
    SELECT team, level
    FROM adjusters
    WHERE team NOT IN ('Health', 'Travel')
    UNION ALL
    SELECT holder_name, id
    FROM policyholders
    WHERE region = 'Central'
      AND joined_on BETWEEN DATE '2024-05-23' AND DATE '2025-05-01'
)
SELECT team, COUNT(*) AS n_rows, SUM(level) AS total_level
FROM all_events
GROUP BY team
ORDER BY team
