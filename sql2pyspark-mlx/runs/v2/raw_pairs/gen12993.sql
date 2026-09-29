SELECT status, 'account' AS source FROM accounts WHERE status IN ('CLOSED', 'OPEN') UNION SELECT status, 'shipment' AS source FROM shipments;
