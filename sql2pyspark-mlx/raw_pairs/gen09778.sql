SELECT
    t.amount,
    t.merchant,
    a.account_type,
    c.name
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
JOIN customers c ON a.customer_id = c.id
WHERE t.merchant = 'Shell'
ORDER BY merchant DESC, t.amount DESC;
