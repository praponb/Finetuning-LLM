SELECT oi.unit_price, o.shipping_fee, p.brand
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON p.id = oi.product_id
ORDER BY shipping_fee DESC
