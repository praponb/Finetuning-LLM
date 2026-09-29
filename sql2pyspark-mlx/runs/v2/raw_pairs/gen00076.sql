WITH account_summary AS (
    SELECT account_id, ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.customer_id,
    as.pct_lazada
FROM accounts a
LEFT JOIN account_summary as ON a.id = as.account_id
WHERE as.pct_lazada >= 34
