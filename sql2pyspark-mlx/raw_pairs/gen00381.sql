WITH dim_totals AS (
    SELECT
        p.segment,
        src.account_type,
        COUNT(*) AS rows_total,
        MIN(src.balance) AS min_balance
    FROM accounts src
    LEFT JOIN customers p ON src.customer_id = p.id
    GROUP BY CUBE (p.segment, src.account_type)
)
SELECT
    segment,
    account_type,
    rows_total,
    min_balance
FROM dim_totals;
