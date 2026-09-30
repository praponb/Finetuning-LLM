SELECT oi.unit_price, o.store_id, o.shipping_fee, o.status FROM order_items oi LEFT JOIN orders o ON o.id = oi.order_id WHERE oi.unit_price < 190
