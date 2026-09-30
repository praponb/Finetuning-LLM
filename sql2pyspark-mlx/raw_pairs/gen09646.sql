SELECT CASE WHEN p.price >= 1290 THEN 'high' WHEN p.price >= 480 THEN 'mid' ELSE 'low' END AS price_bucket, MIN(ss.revenue) AS min_revenue, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
RIGHT JOIN suppliers s ON s.id = p.supplier_id
GROUP BY CASE WHEN p.price >= 1290 THEN 'high' WHEN p.price >= 480 THEN 'mid' ELSE 'low' END
ORDER BY stddev_revenue
