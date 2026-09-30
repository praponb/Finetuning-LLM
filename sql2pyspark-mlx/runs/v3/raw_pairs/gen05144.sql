WITH top_order_items AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_num
    FROM order_items
    WHERE quantity > 5
)
SELECT
    id,
    product_id,
    quantity,
    unit_price
FROM top_order_items
WHERE row_num <= 5;
