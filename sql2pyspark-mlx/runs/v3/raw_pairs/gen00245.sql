WITH ranked_store_sales AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS pos
    FROM store_sales
)
SELECT id, store_id, sale_date
FROM ranked_store_sales
WHERE pos <= 3;
