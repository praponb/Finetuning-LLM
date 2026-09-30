SELECT o.shipping_fee, oi.product_id FROM orders o LEFT OUTER JOIN order_items oi ON o.id = oi.order_id WHERE MONTH(o.order_date) IN (3, 7, 12) AND oi.order_id IN (29, 36, 39)
