SELECT CASE WHEN customers.age >= 69 THEN 'high' WHEN customers.age >= 33 THEN 'mid' ELSE 'low' END AS age_bucket, ROUND(AVG(orders.amount), 2) AS avg_amount
FROM orders
INNER JOIN customers ON orders.customer_id = customers.id
WHERE orders.order_date > '2024-04-11'
GROUP BY CASE WHEN customers.age >= 69 THEN 'high' WHEN customers.age >= 33 THEN 'mid' ELSE 'low' END;
