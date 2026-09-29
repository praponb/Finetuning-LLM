SELECT
    id,
    order_id,
    cost,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) cost_row
FROM shipments
ORDER BY cost_row
