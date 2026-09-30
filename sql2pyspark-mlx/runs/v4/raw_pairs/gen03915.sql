WITH top_order_items AS (
    SELECT
        id,
        product_id,
        unit_price,
        quantity,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) AS rnk
    FROM order_items
)
SELECT
    id,
    product_id,
    unit_price,
    quantity
FROM top_order_items
WHERE rnk <= 3
ORDER BY unit_price DESC
