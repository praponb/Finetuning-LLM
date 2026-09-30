SELECT shipments.cost, shipments.carrier, orders.channel FROM shipments INNER JOIN orders ON orders.id = shipments.order_id;
