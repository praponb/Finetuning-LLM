SELECT DATE_FORMAT(s.opened_date, 'yyyy-MM') AS opened_month, COUNT(*) AS store_sale_count
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY DATE_FORMAT(s.opened_date, 'yyyy-MM')
HAVING store_sale_count > 2
ORDER BY store_sale_count DESC, opened_month
