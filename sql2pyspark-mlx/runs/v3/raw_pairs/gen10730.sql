SELECT
    CASE WHEN units >= 43 THEN 'high' WHEN units >= 19 THEN 'mid' ELSE 'low' END units_bucket,
    product_id,
    MAX(sale_date) latest_sale_date,
    MAX(units) - MIN(units) units_range,
    COUNT(*) cnt
FROM store_sales
WHERE revenue BETWEEN 1300 AND 2600 AND units > 13
GROUP BY CASE WHEN units >= 43 THEN 'high' WHEN units >= 19 THEN 'mid' ELSE 'low' END, product_id
HAVING MAX(units) - MIN(units) >= 39
ORDER BY latest_sale_date ASC;
