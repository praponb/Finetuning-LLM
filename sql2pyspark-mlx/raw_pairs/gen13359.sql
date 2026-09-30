SELECT o.shipping_fee, o.store_id, oi.product_id FROM orders o LEFT OUTER JOIN order_items oi ON o.id = oi.order_id WHERE oi.product_id IS NULL ORDER BY product_id, shipping_fee
