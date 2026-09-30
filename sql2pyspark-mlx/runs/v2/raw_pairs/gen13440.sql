SELECT category, AVG(price) AS avg_price, MAX(launch_date) AS latest_launch_date
FROM products
GROUP BY category
ORDER BY avg_price DESC, category;
