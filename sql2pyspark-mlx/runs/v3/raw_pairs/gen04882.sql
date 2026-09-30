SELECT
    a.id,
    a.opened_date,
    a.customer_id,
    c.age
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
WHERE c.is_active
ORDER BY c.age DESC, a.customer_id DESC
