SELECT start_date FROM subscriptions INTERSECT SELECT delivered_date AS start_date FROM shipments WHERE shipped_date >= '2024-03-01' ORDER BY start_date
