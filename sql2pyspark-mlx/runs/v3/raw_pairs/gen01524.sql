WITH filtered_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE NOT (units <= 16 AND (units <= 39 OR MONTH(sale_date) IN (3, 5, 11)))
),
agg AS (
    SELECT product_id, SUM(revenue) AS total_revenue, MIN(sale_date) AS first_sale_date
    FROM filtered_store_sales
    GROUP BY product_id
)
SELECT *
FROM agg
WHERE total_revenue < 18000
ORDER BY product_id
LIMIT 1
