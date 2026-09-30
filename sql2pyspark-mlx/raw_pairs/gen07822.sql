SELECT p.id, p.status, p.order_id, o.shipping_fee, o.status AS order_status, o.discount FROM payments p LEFT JOIN orders o ON p.order_id = o.id;
