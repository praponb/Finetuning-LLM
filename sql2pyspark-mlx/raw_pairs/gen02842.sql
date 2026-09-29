SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, COUNT(CASE WHEN o.status = 'CANCELLED' THEN 1 END) AS cancelled_count
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM')
HAVING COUNT(CASE WHEN o.status = 'CANCELLED' THEN 1 END) > 2
ORDER BY cancelled_count;
