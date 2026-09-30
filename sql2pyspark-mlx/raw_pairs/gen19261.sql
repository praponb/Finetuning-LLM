SELECT
    a.id,
    a.status,
    COUNT(t.id) AS transaction_count,
    ROUND(AVG(t.amount), 2) AS avg_amount,
    COUNT(t.amount) AS amount_count
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE t.account_id IN (13, 14, 15, 24)
GROUP BY a.id, a.status
ORDER BY a.id DESC
