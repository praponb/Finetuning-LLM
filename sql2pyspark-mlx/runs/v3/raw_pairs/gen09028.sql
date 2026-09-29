SELECT delivered_date, 'shipment' AS source FROM shipments UNION SELECT end_date, 'subscription' AS source FROM subscriptions WHERE end_date < '2023-10-01';
