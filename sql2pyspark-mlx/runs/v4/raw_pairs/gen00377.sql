SELECT status, 'shipment' AS source FROM shipments WHERE shipped_date < '2023-05-01' UNION SELECT status, 'subscription' AS source FROM subscriptions
