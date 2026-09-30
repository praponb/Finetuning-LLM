WITH account_totals AS (
    SELECT account_id, COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count
    FROM transactions
    GROUP BY account_id
)
SELECT a.balance, a.account_type, at.debit_count
FROM accounts a
LEFT OUTER JOIN account_totals at ON a.id = at.account_id
ORDER BY debit_count;
