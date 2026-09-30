SELECT MONTH(s.opened_date) AS opened_month, MAX(ss.revenue) AS max_revenue, SUM(ss.units) AS total_units
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
GROUP BY MONTH(s.opened_date)
ORDER BY total_units DESC;
