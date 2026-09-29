SELECT s.id, s.cost, o.store_id, o.amount, o.order_date FROM shipments s JOIN orders o ON s.order_id = o.id WHERE s.order_id IS NULL OR o.shipping_fee < 16;
