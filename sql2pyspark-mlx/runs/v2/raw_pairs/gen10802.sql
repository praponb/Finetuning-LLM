SELECT
    o.channel,
    COUNT(s.id) AS shipment_count,
    SUM(CASE WHEN s.status = 'IN_TRANSIT' THEN s.cost ELSE 0 END) AS in_transit_cost,
    MIN(s.cost) AS min_cost
FROM orders o
LEFT JOIN shipments s ON o.id = s.order_id
GROUP BY o.channel
ORDER BY 2, in_transit_cost DESC
