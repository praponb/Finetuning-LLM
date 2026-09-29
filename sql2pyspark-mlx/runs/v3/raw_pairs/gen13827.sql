SELECT
    id,
    order_id,
    status,
    carrier,
    CONCAT(status, ' ', carrier) AS status_carrier
FROM shipments
WHERE cost BETWEEN 26 AND 79 OR carrier IN ('UPS', 'Kerry', 'DHL')
ORDER BY id, status DESC;
