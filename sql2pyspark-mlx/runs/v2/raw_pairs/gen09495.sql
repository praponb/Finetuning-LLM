SELECT CASE WHEN s.sqft >= 15218 THEN 'high' WHEN s.sqft >= 7805 THEN 'mid' ELSE 'low' END AS sqft_bucket, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue, SUM(ss.units) AS total_units
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE p.stock_qty = 400
GROUP BY CASE WHEN s.sqft >= 15218 THEN 'high' WHEN s.sqft >= 7805 THEN 'mid' ELSE 'low' END
ORDER BY sqft_bucket DESC
LIMIT 5
