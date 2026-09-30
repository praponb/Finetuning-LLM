SELECT id, delivered_date, status, cost, CONCAT(status, '/', carrier) AS status_carrier FROM shipments WHERE carrier NOT IN ('UPS', 'FedEx');
