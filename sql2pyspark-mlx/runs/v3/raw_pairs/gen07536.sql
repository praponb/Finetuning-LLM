SELECT c.id, c.city, c.age
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.shipping_fee > 21)
ORDER BY age DESC, c.city ASC;
