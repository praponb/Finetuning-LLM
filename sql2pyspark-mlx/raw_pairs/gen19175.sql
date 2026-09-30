SELECT
    CASE WHEN ss.units >= 44 THEN 'high' WHEN ss.units >= 19 THEN 'mid' ELSE 'low' END AS units_bucket,
    MONTH(ss.sale_date) AS sale_month,
    MAX(i.quantity) AS max_quantity,
    SUM(i.quantity) AS total_quantity
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue >= 3500
GROUP BY CASE WHEN ss.units >= 44 THEN 'high' WHEN ss.units >= 19 THEN 'mid' ELSE 'low' END, MONTH(ss.sale_date)
HAVING MAX(i.quantity) > 160
ORDER BY total_quantity DESC, max_quantity DESC;
