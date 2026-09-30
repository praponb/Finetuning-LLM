SELECT QUARTER(order_date) AS order_quarter, COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count
FROM orders
WHERE channel = 'mobile'
GROUP BY QUARTER(order_date)
HAVING COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) >= 2;
