SELECT src.id, src.order_id, src.carrier, src.delivered_date, src.cost, b.amount, b.order_date FROM shipments src JOIN orders b ON src.order_id = b.id;
