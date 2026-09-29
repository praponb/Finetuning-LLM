SELECT category, SUM(price) total_price, ROUND(SUM(price) * 100.0 / SUM(SUM(price)) OVER (), 2) AS pct_of_total
FROM products
WHERE launch_date >= '2023-06-01'
GROUP BY category
ORDER BY total_price DESC, 3;
