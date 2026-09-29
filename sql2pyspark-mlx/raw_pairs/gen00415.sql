SELECT id, status, UPPER(status) AS status_upper
FROM shipments
WHERE delivered_date < '2024-09-12' AND delivered_date < '2025-02-17'
ORDER BY id
