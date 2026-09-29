SELECT s.id, s.cost, s.carrier, o.store_id, o.amount FROM shipments s RIGHT JOIN orders o ON s.order_id = o.id WHERE s.carrier IN ('FedEx', 'Flash', 'UPS');
