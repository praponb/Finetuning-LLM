SELECT YEAR(s.opened_date) AS opened_year, s.city, ROUND(AVG(ss.revenue), 2) AS avg_revenue
FROM store_sales ss
RIGHT JOIN stores s ON ss.store_id = s.id
GROUP BY YEAR(s.opened_date), s.city
HAVING ROUND(AVG(ss.revenue), 2) >= 27
ORDER BY city DESC, opened_year
LIMIT 1
