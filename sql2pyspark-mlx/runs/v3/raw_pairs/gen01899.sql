SELECT s.id, s.status, o.shipping_fee, o.customer_id FROM shipments s INNER JOIN orders o ON o.id = s.order_id WHERE MONTH(s.shipped_date) IN (2, 9, 11);
