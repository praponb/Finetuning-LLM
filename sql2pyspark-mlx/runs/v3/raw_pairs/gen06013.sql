SELECT s.id, s.carrier, o.shipping_fee, o.order_date, o.amount FROM shipments s LEFT OUTER JOIN orders o ON s.order_id = o.id ORDER BY carrier DESC;
