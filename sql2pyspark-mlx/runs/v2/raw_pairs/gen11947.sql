SELECT MONTH(s.opened_date) AS opened_month, MIN(ss.units) AS min_units
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE ss.units BETWEEN 30 AND 34
GROUP BY MONTH(s.opened_date)
ORDER BY min_units DESC;
