SELECT
    id,
    product_id,
    revenue,
    LEAST(revenue, units) AS least_revenue_units
FROM store_sales
WHERE sale_date BETWEEN '2024-03-01' AND '2024-09-01'
ORDER BY product_id DESC
