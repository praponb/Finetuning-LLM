SELECT order_id, shipped_date FROM shipments WHERE status = 'LOST' INTERSECT SELECT order_id, shipped_date FROM shipments WHERE carrier = 'FedEx';
