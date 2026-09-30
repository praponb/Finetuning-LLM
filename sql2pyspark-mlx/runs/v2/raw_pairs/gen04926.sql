SELECT YEAR(order_date) AS order_year, SUM(discount) AS total_discount, SUM(SUM(discount)) OVER (ORDER BY YEAR(order_date)) AS cumulative_discount
FROM orders
GROUP BY YEAR(order_date)
ORDER BY order_year
