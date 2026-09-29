SELECT ss.id, ss.product_id, p.product_name
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
WHERE ss.store_id = 2
ORDER BY product_name;
