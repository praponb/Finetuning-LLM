WITH ranked_order_items AS (
    SELECT
        id,
        product_id,
        unit_price,
        quantity,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) AS rn
    FROM order_items
)
SELECT
    id,
    product_id,
    unit_price,
    quantity
FROM ranked_order_items
WHERE rn <= 3
ORDER BY quantity;
