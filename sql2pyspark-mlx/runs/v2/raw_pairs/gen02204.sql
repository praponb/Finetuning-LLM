SELECT
    o.id,
    COUNT(s.id) AS shipment_count,
    COUNT(CASE WHEN s.carrier = 'Kerry' THEN 1 END) AS kerry_count,
    MAX(s.cost) - MIN(s.cost) AS cost_range
FROM orders o
JOIN shipments s ON o.id = s.order_id
GROUP BY o.id
HAVING COUNT(CASE WHEN s.carrier = 'Kerry' THEN 1 END) > 1;
