WITH top_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS rn
    FROM store_sales
    WHERE product_id = 6
)
SELECT *
FROM top_store_sales
WHERE rn <= 3;
