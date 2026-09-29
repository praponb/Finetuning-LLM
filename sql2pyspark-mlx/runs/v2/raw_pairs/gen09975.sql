SELECT MONTH(order_date) AS order_month, COUNT(discount) AS discount_count
FROM orders
WHERE status != 'COMPLETED' AND amount BETWEEN 1360 AND 1460
GROUP BY 1;
