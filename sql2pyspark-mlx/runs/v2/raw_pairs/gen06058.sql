SELECT s.id, s.delivered_date, s.status, o.shipping_fee, o.order_date, o.amount, o.discount + o.shipping_fee AS discount_plus_shipping_fee FROM shipments s INNER JOIN orders o ON s.order_id = o.id
