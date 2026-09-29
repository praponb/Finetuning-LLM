SELECT
    dim.status AS order_status,
    e.status AS shipment_status,
    COUNT(*) AS rows_total,
    SUM(e.cost) AS total_cost
FROM shipments e
JOIN orders dim ON e.order_id = dim.id
GROUP BY ROLLUP (dim.status, e.status);
