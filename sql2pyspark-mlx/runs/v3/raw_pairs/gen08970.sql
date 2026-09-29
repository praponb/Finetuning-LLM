SELECT
    a.account_type,
    t.is_flagged,
    t.amount,
    t.account_id
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.account_type = 'SAVINGS'
ORDER BY account_id, amount DESC
