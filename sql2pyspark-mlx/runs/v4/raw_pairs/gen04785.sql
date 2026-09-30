SELECT YEAR(p.launch_date) launch_year, COUNT(ss.units) units_count, ROUND(STDDEV(ss.revenue), 2) stddev_revenue
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE s.opened_date BETWEEN '2023-03-01' AND '2023-09-01'
GROUP BY YEAR(p.launch_date)
HAVING ROUND(STDDEV(ss.revenue), 2) >= 72
ORDER BY units_count, launch_year DESC;
