WITH account_summary AS (
    SELECT account_id, ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    a.status,
    as.pct_credit
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id;
