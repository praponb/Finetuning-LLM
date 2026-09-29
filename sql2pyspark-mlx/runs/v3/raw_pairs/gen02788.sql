WITH account_totals AS (
    SELECT account_id, COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.opened_date, a.account_type, at.credit_count
FROM accounts a
LEFT OUTER JOIN account_totals at ON a.id = at.account_id
ORDER BY credit_count DESC, 2;
