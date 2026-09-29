SELECT oi.unit_price, oi.product_id, o.store_id
FROM order_items oi
FULL OUTER JOIN orders o ON oi.order_id = o.id
ORDER BY unit_price DESC, oi.product_id DESC
