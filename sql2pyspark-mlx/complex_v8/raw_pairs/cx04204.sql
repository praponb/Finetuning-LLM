WITH unioned AS (
    -- Stack the branches
    SELECT holder_id AS qty, id AS qty_2, 'policy' AS src
    FROM policies
    WHERE status IN ('LAPSED', 'EXPIRED', 'ACTIVE')
    UNION ALL
    SELECT level, id, 'adjuster' AS src
    FROM adjusters
    UNION ALL
    SELECT holder_id, id, 'policy_3' AS src
    FROM policies
    WHERE status IN ('LAPSED', 'ACTIVE')
)
SELECT src, COUNT(*) AS n_rows, SUM(qty_2) AS total_qty_2
FROM unioned
GROUP BY src
ORDER BY src
