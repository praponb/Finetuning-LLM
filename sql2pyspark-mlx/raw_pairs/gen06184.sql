SELECT c.is_active, o.amount
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
WHERE o.channel IN ('partner', 'web')
