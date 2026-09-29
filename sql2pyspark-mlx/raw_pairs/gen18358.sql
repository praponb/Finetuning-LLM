SELECT order_id FROM shipments EXCEPT ALL SELECT order_id FROM shipments WHERE carrier IN ('Kerry', 'DHL', 'FedEx');
