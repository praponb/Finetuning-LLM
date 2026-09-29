WITH top_orders AS (
    SELECT
        id,
        channel,
        discount,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id) AS rnk
    FROM orders
    WHERE discount < 0.27
)
SELECT
    id,
    channel,
    discount,
    customer_id
FROM top_orders
WHERE rnk <= 3;
