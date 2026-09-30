SELECT p.category, MAX(ss.sale_date) AS latest_sale_date
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE ss.store_id IN (3, 5, 6)
GROUP BY p.category
