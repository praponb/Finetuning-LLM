SELECT ss.product_id, s.city, s.opened_date
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
ORDER BY opened_date;
