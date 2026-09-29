WITH top_order_items AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num
    FROM order_items
    WHERE unit_price > 210
)
SELECT id, product_id, quantity
FROM top_order_items
WHERE row_num <= 5
