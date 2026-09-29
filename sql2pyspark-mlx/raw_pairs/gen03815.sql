SELECT order_id
FROM shipments
WHERE carrier = 'DHL' OR order_id IS NOT NULL
ORDER BY order_id DESC;
