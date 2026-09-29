SELECT status, 'shipment' AS source FROM shipments WHERE MONTH(shipped_date) IN (1, 3, 12) UNION ALL SELECT status, 'subscription' AS source FROM subscriptions ORDER BY source DESC;
