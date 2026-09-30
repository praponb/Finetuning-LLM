SELECT
    status,
    order_id,
    carrier,
    COUNT(*) AS row_count
FROM shipments
WHERE cost > 40 AND carrier IN ('Kerry', 'FedEx')
GROUP BY status, order_id, carrier
ORDER BY status;
