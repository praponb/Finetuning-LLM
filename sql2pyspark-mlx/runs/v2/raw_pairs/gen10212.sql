SELECT s.id, s.cost, s.order_id, s.delivered_date, o.order_date, o.amount FROM shipments s JOIN orders o ON s.order_id = o.id
