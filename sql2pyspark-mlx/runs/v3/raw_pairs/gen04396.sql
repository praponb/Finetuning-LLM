SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    COUNT(DISTINCT t.amount) AS unique_merchants
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE a.status IN ('OPEN', 'CLOSED', 'FROZEN')
GROUP BY a.id, a.account_type
HAVING transaction_count > 3
ORDER BY id DESC;
