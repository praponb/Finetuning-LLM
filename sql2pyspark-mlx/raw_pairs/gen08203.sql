WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        d.city,
        d.country,
        src.account_type,
        COUNT(*) AS num_rows,
        SUM(src.balance) AS total_balance
    FROM accounts src
    JOIN customers d ON src.customer_id = d.id
    GROUP BY GROUPING SETS ((d.city, d.country, src.account_type), (d.city))
)
SELECT
    country,
    account_type,
    CASE WHEN account_type RLIKE '^(CREDIT|CHECKING)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS account_type_zone
FROM dim_totals
ORDER BY country NULLS FIRST;
