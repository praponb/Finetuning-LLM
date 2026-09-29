SELECT
    QUARTER(launch_date) AS launch_quarter,
    SUM(stock_qty) AS total_stock_qty,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(stock_qty) DESC) AS rnk
FROM products
WHERE price >= 360
GROUP BY QUARTER(launch_date)
ORDER BY launch_quarter DESC;
