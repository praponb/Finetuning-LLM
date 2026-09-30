SELECT category, MIN(launch_date) AS min_launch_date
FROM products
WHERE (product_name LIKE '%Run%' AND stock_qty >= 322) OR category != 'Fashion'
GROUP BY category
ORDER BY min_launch_date DESC;
