WITH src AS (
    SELECT *, CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE units BETWEEN 21 AND 30 AND units < 22 AND revenue > 2200
),
store_sales_stats AS (
    SELECT store_id, COUNT(DISTINCT revenue_int) AS unique_products
    FROM src
    GROUP BY store_id
)
SELECT store_id, unique_products
FROM store_sales_stats
ORDER BY unique_products DESC
