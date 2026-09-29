SELECT status, 'account' AS source FROM accounts WHERE account_type IN ('SAVINGS', 'CREDIT', 'LOAN') UNION ALL SELECT status, 'shipment' AS source FROM shipments WHERE carrier = 'Kerry';
