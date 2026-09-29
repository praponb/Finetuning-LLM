SELECT id, carrier, shipped_date FROM shipments WHERE delivered_date >= '2025-04-01' UNION ALL SELECT id, carrier, shipped_date FROM shipments WHERE status <> 'LOST';
