SELECT QUARTER(c.signup_date) signup_quarter, c.city, AVG(o.discount) avg_discount
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
WHERE o.amount IS NOT NULL
GROUP BY QUARTER(c.signup_date), c.city
HAVING avg_discount >= 0.18
