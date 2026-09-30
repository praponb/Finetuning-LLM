SELECT p.order_id, p.paid_at, o.customer_id, o.shipping_fee FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id ORDER BY p.order_id DESC
