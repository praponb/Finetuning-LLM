SELECT a.status, t.account_id
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
ORDER BY account_id DESC
