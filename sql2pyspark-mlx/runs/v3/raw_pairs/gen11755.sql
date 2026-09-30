SELECT
    t.id,
    t.merchant,
    a.opened_date,
    c.is_active
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
ORDER BY opened_date DESC
