WITH unioned AS (
    -- Stack the branches
    SELECT region AS category, joined_on, 'holder' AS origin
    FROM policyholders
    WHERE segment IN ('VIP', 'RETAIL')
    UNION ALL
    SELECT status AS category, end_date AS joined_on, 'policy' AS origin
    FROM policies
    WHERE end_date >= DATE '2026-02-26'
    UNION ALL
    SELECT status AS category, start_date AS joined_on, 'policy' AS origin
    FROM policies
)
SELECT origin, COUNT(*) AS n_rows, MAX(joined_on) AS latest_joined_on
FROM unioned
GROUP BY origin
ORDER BY origin
