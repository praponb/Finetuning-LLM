SELECT s.id, s.delivered_date, s.shipped_date
FROM shipments s
WHERE s.shipped_date BETWEEN '2023-05-01' AND '2023-08-29' AND s.order_id NOT IN (SELECT id FROM orders WHERE discount BETWEEN 0.14 AND 0.31);
