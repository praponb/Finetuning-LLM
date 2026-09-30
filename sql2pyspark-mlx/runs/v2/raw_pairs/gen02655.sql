SELECT id, status, COUNT(*) OVER (PARTITION BY status) AS status_count
FROM orders
ORDER BY status DESC, id DESC
