SELECT YEAR(launch_date) AS launch_year, SUM(price) AS total_price, SUM(SUM(price)) OVER (ORDER BY YEAR(launch_date)) AS cumulative_price
FROM products
WHERE launch_date > '2024-07-01'
GROUP BY YEAR(launch_date);
