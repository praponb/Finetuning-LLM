SELECT end_date, 'subscription' source FROM subscriptions WHERE end_date < '2024-07-01' UNION SELECT shipped_date end_date, 'shipment' source FROM shipments
