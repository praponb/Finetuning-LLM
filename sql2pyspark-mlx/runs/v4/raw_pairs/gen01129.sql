SELECT MONTH(order_date) AS order_month, SUM(amount) AS total_amount, ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 2) AS pct_of_total
FROM orders
GROUP BY MONTH(order_date)
ORDER BY order_month DESC
