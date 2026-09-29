WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num
    FROM order_items
)
SELECT
    id,
    product_id,
    quantity,
    unit_price
FROM ranked
WHERE row_num = 1
ORDER BY product_id DESC;
