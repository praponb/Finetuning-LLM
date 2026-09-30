SELECT DISTINCT channel, status
FROM orders
WHERE YEAR(order_date) = 2023
ORDER BY 2;
