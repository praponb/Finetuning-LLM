SELECT s.id, s.carrier, s.delivered_date, o.status FROM shipments s JOIN orders o ON s.order_id = o.id WHERE YEAR(s.delivered_date) = 2024 AND o.amount IS NOT NULL ORDER BY id DESC;
