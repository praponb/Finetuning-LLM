SELECT id, order_id
FROM payments
WHERE amount IS NOT NULL
INTERSECT
SELECT id, order_id
FROM payments
WHERE method IS NULL
ORDER BY order_id;
