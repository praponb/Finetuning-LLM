SELECT
    product_id,
    CASE WHEN units >= 37 THEN 'high' WHEN units >= 16 THEN 'mid' ELSE 'low' END AS units_bucket,
    ROUND(AVG(units), 2) AS avg_units,
    AVG(revenue) AS avg_revenue
FROM store_sales
WHERE sale_date BETWEEN '2023-10-01' AND '2024-07-01'
GROUP BY product_id, units_bucket
HAVING COUNT(revenue) < 3;
