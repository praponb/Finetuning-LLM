SELECT COUNT(DISTINCT revenue) AS unique_products, MAX(revenue) - MIN(revenue) AS revenue_range
FROM store_sales
WHERE revenue <= 4000
