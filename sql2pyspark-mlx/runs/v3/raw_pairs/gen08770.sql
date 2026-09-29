SELECT shipments.id, shipments.shipped_date, shipments.cost, orders.order_date, orders.status FROM shipments LEFT OUTER JOIN orders ON shipments.order_id = orders.id;
