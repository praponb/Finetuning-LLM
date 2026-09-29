SELECT o.discount, o.store_id, c.is_active, c.age, c.email, DATE_ADD(c.signup_date, 31) AS signup_date_plus_31d FROM orders o RIGHT JOIN customers c ON c.id = o.customer_id WHERE o.shipping_fee <= 35
