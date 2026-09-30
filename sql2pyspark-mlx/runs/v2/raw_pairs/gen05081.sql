SELECT
    CASE WHEN revenue >= 3600 THEN 'high' WHEN revenue >= 1700 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    AVG(revenue) AS avg_revenue,
    MIN(sale_date) AS first_sale_date,
    MAX(revenue) AS max_revenue
FROM store_sales
WHERE product_id IN (2, 4, 6, 7)
GROUP BY 1;
