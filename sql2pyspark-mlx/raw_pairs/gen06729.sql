SELECT YEAR(p.launch_date) AS launch_year, MAX(ss.units) - MIN(ss.units) AS units_range, SUM(ss.units) AS total_units
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
WHERE ss.revenue <= 1900
GROUP BY YEAR(p.launch_date)
HAVING SUM(ss.units) > 160
ORDER BY total_units;
