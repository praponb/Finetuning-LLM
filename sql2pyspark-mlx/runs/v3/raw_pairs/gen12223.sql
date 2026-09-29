SELECT brand, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY brand) AS cumulative_cost FROM products WHERE launch_date BETWEEN '2024-04-01' AND '2024-08-01' GROUP BY brand
