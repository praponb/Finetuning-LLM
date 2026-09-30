SELECT o.id, o.discount, o.order_date, o.shipping_fee FROM orders o WHERE EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id) ORDER BY shipping_fee
