SELECT YEAR(launch_date) AS launch_year, MIN(stock_qty) AS min_stock_qty, SUM(CASE WHEN category = 'Sports' THEN cost ELSE 0 END) AS sports_cost
FROM products
WHERE stock_qty >= 255 AND (stock_qty BETWEEN 218 AND 348 OR category IN ('Books', 'Electronics', 'Grocery'))
GROUP BY YEAR(launch_date)
