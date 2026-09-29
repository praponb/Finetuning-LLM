SELECT YEAR(launch_date) AS launch_year, ROUND(AVG(cost), 2) AS avg_cost, SUM(stock_qty) AS total_stock_qty
FROM products
WHERE launch_date BETWEEN '2024-03-05' AND '2025-01-10' OR cost < 470
GROUP BY YEAR(launch_date)
