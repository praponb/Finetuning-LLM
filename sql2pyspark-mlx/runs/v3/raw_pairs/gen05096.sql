SELECT
    c.segment,
    c.credit_limit,
    c.is_active,
    a.status
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
ORDER BY segment, a.status DESC
