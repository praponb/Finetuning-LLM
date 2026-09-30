SELECT p.supplier_id, p.brand, COUNT(DISTINCT ss.revenue) AS unique_products
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
GROUP BY p.supplier_id, p.brand
HAVING COUNT(DISTINCT ss.revenue) > 2
