SELECT
    id,
    order_id,
    cost,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) AS cost_row
FROM shipments
WHERE cost BETWEEN 35 AND 66;
