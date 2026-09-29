SELECT o.id, o.amount
FROM orders o
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price > 110)
ORDER BY amount DESC, id
LIMIT 10;
