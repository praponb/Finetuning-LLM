WITH filtered_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE store_id IN (1, 3, 4, 5) AND units <> 39
),
store_sales_stats AS (
    SELECT store_id, COUNT(DISTINCT revenue) AS unique_stores, COUNT(*) AS num_store_sales
    FROM filtered_store_sales
    GROUP BY store_id
)
SELECT store_id, unique_stores, num_store_sales
FROM store_sales_stats
WHERE num_store_sales > 9
ORDER BY unique_stores, num_store_sales DESC;
