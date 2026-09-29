SELECT MONTH(c.signup_date) AS signup_month, MAX(o.discount) AS max_discount FROM orders o LEFT JOIN customers c ON o.customer_id = c.id GROUP BY MONTH(c.signup_date) ORDER BY signup_month;
