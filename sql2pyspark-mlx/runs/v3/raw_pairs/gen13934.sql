SELECT QUARTER(c.signup_date) AS signup_quarter, COUNT(*) AS num_orders
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON s.id = o.store_id
WHERE o.status != 'PENDING'
GROUP BY QUARTER(c.signup_date);
