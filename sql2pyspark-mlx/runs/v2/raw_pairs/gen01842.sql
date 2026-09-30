SELECT category, brand, ROUND(AVG(price), 2) avg_price FROM products WHERE launch_date > '2023-11-01' GROUP BY category, brand HAVING ROUND(AVG(price), 2) > 26;
