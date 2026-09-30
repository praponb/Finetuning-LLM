SELECT s.city, MIN(ss.sale_date) AS first_sale_date
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY s.city
ORDER BY s.city DESC, first_sale_date;
