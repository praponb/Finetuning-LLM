SELECT status, 'shipment' source FROM shipments WHERE shipped_date BETWEEN '2023-08-01' AND '2023-09-01' UNION ALL SELECT status, 'subscription' source FROM subscriptions ORDER BY status;
