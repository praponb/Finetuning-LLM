SELECT id, carrier, CONCAT(status, ' ', carrier) status_carrier
FROM shipments
WHERE shipped_date > '2025-09-07'
ORDER BY id DESC;
