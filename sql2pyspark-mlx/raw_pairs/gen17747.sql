SELECT
    a.id,
    a.account_type,
    COUNT(t.id) AS transaction_count,
    COUNT(DISTINCT t.amount) AS unique_accounts,
    MAX(t.amount) - MIN(t.amount) AS amount_range
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
GROUP BY a.id, a.account_type
ORDER BY unique_accounts DESC, 5 DESC
