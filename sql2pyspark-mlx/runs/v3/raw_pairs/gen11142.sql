SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, COUNT(DISTINCT amount) AS unique_stores
FROM orders
GROUP BY order_month
HAVING COUNT(DISTINCT amount) >= 4;
