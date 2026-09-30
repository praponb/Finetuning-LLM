SELECT YEAR(order_date) AS order_year, SUM(CASE WHEN status = 'SHIPPED' THEN amount ELSE 0 END) AS shipped_amount
FROM orders
GROUP BY YEAR(order_date)
HAVING SUM(CASE WHEN status = 'SHIPPED' THEN amount ELSE 0 END) >= 27
