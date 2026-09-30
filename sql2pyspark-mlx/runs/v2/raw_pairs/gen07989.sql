SELECT
    product_id,
    CASE WHEN revenue >= 3700 THEN 'high' WHEN revenue >= 600 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    MAX(sale_date) AS latest_sale_date,
    COUNT(DISTINCT revenue) AS unique_stores,
    MAX(revenue) - MIN(revenue) AS revenue_range
FROM store_sales
WHERE revenue >= 1400
GROUP BY 1, 2
ORDER BY unique_stores DESC
