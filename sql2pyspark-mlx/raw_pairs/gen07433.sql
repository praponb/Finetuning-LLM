WITH ranked_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS rnk
    FROM store_sales
)
SELECT *
FROM ranked_store_sales
WHERE rnk <= 3
ORDER BY rnk;
