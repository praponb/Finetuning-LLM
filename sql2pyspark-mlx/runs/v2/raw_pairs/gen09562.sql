WITH ranked AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS rnk
    FROM order_items
)
SELECT
    id,
    order_id,
    quantity,
    unit_price
FROM ranked
WHERE rnk = 1
ORDER BY unit_price DESC;
