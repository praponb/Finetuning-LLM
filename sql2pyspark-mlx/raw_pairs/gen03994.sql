SELECT
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    CASE WHEN units >= 35 THEN 'high' WHEN units >= 12 THEN 'mid' ELSE 'low' END AS units_bucket,
    SUM(units) AS total_units,
    COUNT(units) AS units_count
FROM store_sales
WHERE product_id IN (3, 4, 9, 10) AND units BETWEEN 17 AND 25
GROUP BY CUBE(DATE_FORMAT(sale_date, 'yyyy-MM'), CASE WHEN units >= 35 THEN 'high' WHEN units >= 12 THEN 'mid' ELSE 'low' END)
