SELECT p.supplier_id, COUNT(*) AS store_sale_count
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
LEFT JOIN stores s ON ss.store_id = s.id
WHERE ss.store_id IN (2, 3, 6)
GROUP BY p.supplier_id
HAVING COUNT(*) > 1
ORDER BY store_sale_count DESC;
