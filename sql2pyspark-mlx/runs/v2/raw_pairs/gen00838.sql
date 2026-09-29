SELECT oi.id, oi.product_id
FROM order_items oi
WHERE oi.order_id IN (SELECT id FROM orders WHERE channel NOT IN ('partner', 'mobile'))
ORDER BY product_id DESC
