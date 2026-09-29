SELECT *
FROM shipments
WHERE status <> 'LOST'
ORDER BY order_id DESC, id ASC
LIMIT 5;
