SELECT start_date FROM subscriptions WHERE customer_id IS NOT NULL UNION ALL SELECT delivered_date FROM shipments
