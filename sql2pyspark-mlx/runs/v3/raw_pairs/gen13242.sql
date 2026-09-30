WITH filtered_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE sale_date BETWEEN '2024-06-13' AND '2025-08-01'
),
agg AS (
    SELECT product_id, COUNT(*) num_store_sales
    FROM filtered_store_sales
    GROUP BY product_id
)
SELECT *
FROM agg
WHERE num_store_sales > 4
ORDER BY num_store_sales DESC
