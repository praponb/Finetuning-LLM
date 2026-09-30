SELECT s.shipped_date, o.order_date, o.status FROM shipments s INNER JOIN orders o ON s.order_id = o.id;
