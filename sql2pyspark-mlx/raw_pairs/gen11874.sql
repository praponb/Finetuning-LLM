SELECT s.cost, o.order_date, o.discount, o.customer_id FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE s.delivered_date BETWEEN '2024-04-01' AND '2024-07-01' ORDER BY cost DESC;
