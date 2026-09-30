WITH by_dimension AS (
    -- Join a dimension and aggregate
    SELECT
        c.country,
        s.status,
        COUNT(*) AS rows_total,
        SUM(s.balance) AS total_balance,
        GROUPING_ID() AS level_id
    FROM accounts s
    LEFT JOIN customers c ON s.customer_id = c.id
    GROUP BY GROUPING SETS ((c.country, s.status), (c.country))
)
SELECT
    status,
    rows_total,
    total_balance,
    level_id,
    CASE WHEN status RLIKE '^(CLOSED|FROZEN)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone
FROM by_dimension;
