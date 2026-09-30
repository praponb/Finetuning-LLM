SELECT
    id,
    order_id,
    delivered_date,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY delivered_date DESC) delivered_date_dense_rank
FROM shipments;
