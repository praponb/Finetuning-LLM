SELECT s.store_name, o.channel, o.order_date
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id
WHERE o.store_id IN (12, 23, 32, 36)
ORDER BY store_name, order_date DESC
