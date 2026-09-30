SELECT status FROM shipments WHERE delivered_date >= '2025-01-01' INTERSECT SELECT status FROM subscriptions ORDER BY status
