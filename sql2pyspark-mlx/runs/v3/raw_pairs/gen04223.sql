SELECT c.age, c.signup_date, o.amount FROM customers c LEFT JOIN orders o ON c.id = o.customer_id ORDER BY amount DESC;
