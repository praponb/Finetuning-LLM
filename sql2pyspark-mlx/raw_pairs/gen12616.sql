SELECT
    d.channel,
    e.status AS shipment_status,
    COUNT(*) AS num_rows,
    MIN(e.cost) AS min_cost,
    GROUPING_ID() AS grouping_level
FROM shipments e
JOIN orders d ON e.order_id = d.id
GROUP BY GROUPING SETS ((d.channel, e.status), (d.channel), ())
ORDER BY shipment_status;
