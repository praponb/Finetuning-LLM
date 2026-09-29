SELECT s.country, MIN(ss.sale_date) AS min_sale_date
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE ss.product_id IN (1, 3, 5, 8)
GROUP BY s.country
ORDER BY min_sale_date DESC;
