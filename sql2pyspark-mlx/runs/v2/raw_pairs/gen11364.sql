SELECT o.id, o.discount
FROM orders o
WHERE EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id)
ORDER BY o.discount DESC
