SELECT
    carrier,
    COUNT(*) AS shipment_count,
    SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) AS flash_cost,
    MAX(cost) AS max_cost
FROM shipments
GROUP BY carrier
ORDER BY flash_cost DESC
