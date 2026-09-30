SELECT o.id, o.amount, o.order_date, o.discount, c.city, c.signup_date FROM orders o RIGHT OUTER JOIN customers c ON o.customer_id = c.id WHERE c.age = 53 OR o.amount BETWEEN 1420 AND 1500;
