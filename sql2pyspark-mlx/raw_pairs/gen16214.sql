SELECT c.email, c.is_active, o.status
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE c.country <> 'SG' AND c.country = 'UK';
