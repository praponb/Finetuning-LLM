SELECT id, order_id, shipped_date
FROM shipments
WHERE NOT (cost >= 28 AND delivered_date < '2025-02-08')
ORDER BY order_id DESC, id
LIMIT 10;
