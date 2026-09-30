SELECT c.id, c.country
FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.shipping_fee BETWEEN 27 AND 30)
