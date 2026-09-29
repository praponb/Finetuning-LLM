SELECT ss.sale_date, s.city, s.opened_date
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE YEAR(s.opened_date) = 2024
ORDER BY sale_date DESC;
