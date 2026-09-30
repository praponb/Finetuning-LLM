SELECT c.id, c.segment
FROM customers c
WHERE c.id IN (SELECT customer_id FROM sessions WHERE browser IS NOT NULL)
ORDER BY id, segment DESC
