WITH src AS (
    SELECT *
    FROM store_sales
    WHERE product_id = 1
),
agg AS (
    SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, COUNT(DISTINCT units) AS unique_stores
    FROM src
    GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT sale_cohort, unique_stores
FROM agg
ORDER BY unique_stores DESC;
