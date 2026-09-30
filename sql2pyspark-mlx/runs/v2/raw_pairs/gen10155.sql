SELECT MONTH(order_date) AS order_month, SUM(amount) AS total_amount, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM orders GROUP BY MONTH(order_date)
