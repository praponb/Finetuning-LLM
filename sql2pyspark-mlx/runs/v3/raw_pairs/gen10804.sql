SELECT
    a.status,
    a.account_type,
    t.avg_amount,
    t.unique_accounts
FROM accounts a
JOIN (SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount, COUNT(DISTINCT amount) AS unique_accounts
    FROM transactions
    WHERE TO_DATE(txn_time) >= '2025-06-27'
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.unique_accounts >= 4
ORDER BY unique_accounts DESC
