SELECT id, status, COUNT(*) OVER (PARTITION BY status) AS status_count
FROM subscriptions
