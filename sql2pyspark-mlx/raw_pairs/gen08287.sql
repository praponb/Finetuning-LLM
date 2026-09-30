WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        d.city,
        e.account_type,
        COUNT(*) AS rows_total,
        MIN(e.balance) AS min_balance
    FROM accounts e
    INNER JOIN customers d ON e.customer_id = d.id
    GROUP BY d.city, e.account_type
)
SELECT
    city,
    account_type,
    rows_total,
    min_balance
FROM rollup_totals
ORDER BY min_balance, rows_total;
