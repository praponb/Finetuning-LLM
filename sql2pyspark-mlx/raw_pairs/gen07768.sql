SELECT p.category, COUNT(ss.id) AS store_sale_count
FROM products p
LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id
WHERE p.supplier_id IS NULL
GROUP BY p.category
ORDER BY p.category, 2;
