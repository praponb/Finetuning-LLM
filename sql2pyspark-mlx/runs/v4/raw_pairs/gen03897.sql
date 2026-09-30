SELECT MIN(shipped_date) first_shipped_date FROM shipments WHERE delivered_date >= '2024-11-01' AND carrier IN ('UPS', 'Kerry') ORDER BY first_shipped_date ASC;
