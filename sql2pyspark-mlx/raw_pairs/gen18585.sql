SELECT p.brand, ROUND(AVG(ss.revenue), 2) avg_revenue
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE s.store_name LIKE '%Expr%'
GROUP BY p.brand
HAVING ROUND(AVG(ss.revenue), 2) >= 55
ORDER BY avg_revenue DESC;
