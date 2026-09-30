SELECT o.id, o.discount, o.shipping_fee, o.amount FROM orders o WHERE MONTH(o.order_date) IN (5, 6, 12) AND EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price IS NULL)
