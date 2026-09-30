SELECT c.id, c.segment, c.name
FROM customers c
WHERE c.city IN ('Singapore', 'Bangkok', 'New York') AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
ORDER BY id DESC;
