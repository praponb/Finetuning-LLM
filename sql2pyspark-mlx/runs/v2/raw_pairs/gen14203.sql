WITH account_summary AS (
    SELECT account_id, ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.account_type,
    a.customer_id,
    as.pct_credit
FROM accounts a
JOIN account_summary as ON a.id = as.account_id
ORDER BY 2 ASC, id ASC
LIMIT 10;
