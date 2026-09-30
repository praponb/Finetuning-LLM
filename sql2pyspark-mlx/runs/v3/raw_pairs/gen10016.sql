SELECT
    id,
    units,
    ROUND(revenue * 0.9, 2) AS adjusted_revenue,
    revenue + units AS revenue_per_unit
FROM store_sales
WHERE sale_date BETWEEN '2024-01-01' AND '2024-09-11' AND sale_date BETWEEN '2023-06-07' AND '2024-06-22'
