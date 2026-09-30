SELECT p.brand, COUNT(DISTINCT ss.revenue) AS unique_products
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
RIGHT OUTER JOIN stores s ON ss.store_id = s.id
GROUP BY p.brand
HAVING COUNT(DISTINCT ss.revenue) >= 3;
