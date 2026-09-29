WITH account_summary AS (
    SELECT account_id, COUNT(*) total_count, ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_credit
    FROM transactions
    WHERE is_flagged
    GROUP BY account_id
)
SELECT
    a.account_type,
    a.opened_date,
    as.total_count,
    as.pct_credit
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id
