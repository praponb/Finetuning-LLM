SELECT o.store_id, o.shipping_fee, o.order_date, oi.unit_price, oi.quantity FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE oi.order_id IN (7, 30, 35, 36);
