SELECT category, launch_month, total_price
FROM (
    SELECT category, MONTH(launch_date) launch_month, SUM(price) total_price
    FROM products
    GROUP BY category, MONTH(launch_date)
) s
WHERE total_price < 5000
