SELECT
    a.id,
    a.balance,
    a.status,
    t.shell_count,
    t.amount_count
FROM accounts a
JOIN (SELECT account_id, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count, COUNT(amount) AS amount_count
    FROM transactions
    WHERE txn_type = 'REFUND'
    GROUP BY account_id) t ON a.id = t.account_id
WHERE t.amount_count >= 9
ORDER BY t.amount_count DESC
