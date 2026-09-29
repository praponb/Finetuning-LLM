WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS row_num
    FROM order_items
)
SELECT id, order_id, quantity
FROM ranked
WHERE row_num <= 2
