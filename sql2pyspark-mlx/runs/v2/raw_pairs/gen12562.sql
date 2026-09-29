SELECT
    id,
    carrier,
    status,
    order_id,
    CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END AS is_in_transit,
    CAST(cost AS INT) AS cost_int
FROM shipments;
