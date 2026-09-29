SELECT
    id,
    status,
    order_id,
    cost,
    CASE WHEN cost >= 83 THEN 'High' WHEN cost >= 41 THEN 'Medium' ELSE 'Low' END AS cost_level
FROM shipments;
