SELECT end_date, 'subscription' AS source FROM subscriptions UNION SELECT delivered_date, 'shipment' AS source FROM shipments WHERE MONTH(delivered_date) IN (3, 7, 9) ORDER BY end_date DESC
