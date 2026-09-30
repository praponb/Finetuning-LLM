SELECT YEAR(order_date) AS order_year, SUM(amount) AS total_amount, SUM(SUM(amount)) OVER (ORDER BY YEAR(order_date)) AS cumulative_amount
FROM orders
GROUP BY YEAR(order_date)
ORDER BY cumulative_amount DESC, order_year;
