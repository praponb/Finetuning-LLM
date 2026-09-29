WITH ranked_orders AS (
    SELECT
        id,
        channel,
        order_date,
        store_id,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY order_date DESC) AS row_num
    FROM orders
)
SELECT
    id,
    channel,
    order_date,
    store_id
FROM ranked_orders
WHERE row_num <= 3;
