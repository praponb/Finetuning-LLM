SELECT status, 'account' AS source FROM accounts WHERE account_type != 'CHECKING' UNION SELECT status, 'shipment' AS source FROM shipments WHERE shipped_date >= '2025-06-01' ORDER BY source;
