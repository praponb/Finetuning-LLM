SELECT YEAR(s.opened_date) AS opened_year, COUNT(*) AS store_sale_count
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE s.sqft = 9793
GROUP BY YEAR(s.opened_date)
ORDER BY store_sale_count, opened_year DESC
LIMIT 10;
