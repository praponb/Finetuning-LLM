SELECT
    o.id,
    o.status,
    o.discount,
    o.amount,
    c.is_active
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'RETURNED' OR LOWER(TRIM(c.email)) = 'carol@company.co';
