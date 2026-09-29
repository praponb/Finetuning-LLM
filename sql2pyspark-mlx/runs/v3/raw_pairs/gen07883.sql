SELECT YEAR(launch_date) AS launch_year, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY YEAR(launch_date)) AS cumulative_cost
FROM products
WHERE launch_date BETWEEN '2023-03-14' AND '2024-02-01'
GROUP BY YEAR(launch_date)
