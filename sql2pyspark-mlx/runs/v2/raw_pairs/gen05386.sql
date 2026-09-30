SELECT i.id, i.product_id, ss.store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE YEAR(ss.sale_date) = 2023
ORDER BY ss.store_id DESC, i.id DESC;
