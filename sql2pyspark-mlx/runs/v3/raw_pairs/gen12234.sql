SELECT
    id,
    carrier,
    shipped_date,
    cost,
    order_id,
    cost - LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS cost_change,
    RANK() OVER (PARTITION BY order_id ORDER BY cost) AS rnk
FROM shipments;
