SELECT
    id,
    cost,
    order_id,
    delivered_date,
    ROW_NUMBER() OVER (ORDER BY cost, id) AS cost_row,
    RANK() OVER (PARTITION BY order_id ORDER BY delivered_date) AS rnk
FROM shipments;
