SELECT
    status,
    SUM(CASE WHEN status = 'IN_TRANSIT' THEN cost ELSE 0 END) AS in_transit_cost,
    SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END) AS ups_count,
    MAX(shipped_date) AS last_shipped_date
FROM shipments
GROUP BY status
HAVING in_transit_cost >= 47
