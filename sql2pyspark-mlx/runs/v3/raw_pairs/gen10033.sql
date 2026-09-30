SELECT brand, YEAR(launch_date) AS launch_year, AVG(cost) AS avg_cost
FROM products
GROUP BY brand, launch_year
ORDER BY avg_cost DESC
