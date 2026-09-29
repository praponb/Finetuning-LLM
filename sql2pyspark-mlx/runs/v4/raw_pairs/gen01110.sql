SELECT shipments.id, shipments.delivered_date, orders.amount, orders.discount FROM shipments FULL OUTER JOIN orders ON shipments.order_id = orders.id ORDER BY orders.amount;
