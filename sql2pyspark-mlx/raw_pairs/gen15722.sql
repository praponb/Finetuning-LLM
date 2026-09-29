SELECT QUARTER(order_date) AS order_quarter, MAX(order_date) AS latest_order_date, COUNT(*) AS cnt
FROM orders
GROUP BY order_quarter
HAVING COUNT(*) > 4
