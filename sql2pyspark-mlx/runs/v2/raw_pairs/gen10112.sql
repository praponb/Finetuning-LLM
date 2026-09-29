WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS rnk
    FROM store_sales
)
SELECT id, product_id, revenue
FROM ranked
WHERE rnk = 1
