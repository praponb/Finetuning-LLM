SELECT o.store_id, MAX(oi.unit_price) AS max_unit_price, COUNT(*) AS cnt
FROM order_items oi
RIGHT JOIN orders o ON oi.order_id = o.id
GROUP BY o.store_id
ORDER BY max_unit_price, cnt;
