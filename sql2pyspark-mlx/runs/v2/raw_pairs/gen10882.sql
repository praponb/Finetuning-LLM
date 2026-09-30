SELECT
    status,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count
FROM shipments
GROUP BY status
ORDER BY stddev_cost DESC
