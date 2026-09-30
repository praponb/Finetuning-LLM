WITH Src AS (
    SELECT *, revenue * units AS revenue_per_unit
    FROM store_sales
    WHERE product_id IN (2, 5, 6) AND sale_date BETWEEN '2024-05-22' AND '2025-04-16'
),
store_id_summary AS (
    SELECT store_id, SUM(revenue_per_unit) AS total_revenue_per_unit, MAX(revenue_per_unit) AS max_revenue_per_unit
    FROM Src
    GROUP BY store_id
)
SELECT store_id, total_revenue_per_unit, max_revenue_per_unit
FROM store_id_summary
WHERE max_revenue_per_unit < 44
ORDER BY store_id DESC
