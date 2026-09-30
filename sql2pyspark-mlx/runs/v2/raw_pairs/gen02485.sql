WITH top_order_items AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS rn
    FROM order_items
)
SELECT
    id,
    product_id,
    quantity,
    unit_price
FROM top_order_items
WHERE rn <= 5
