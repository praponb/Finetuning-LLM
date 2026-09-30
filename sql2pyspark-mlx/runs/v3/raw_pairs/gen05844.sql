SELECT ss.store_id, ss.product_id, s.city
FROM store_sales ss
RIGHT JOIN stores s ON ss.store_id = s.id
ORDER BY ss.product_id DESC
