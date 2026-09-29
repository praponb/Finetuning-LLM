SELECT c.city, c.country, c.signup_date, s.country AS session_country FROM customers c INNER JOIN sessions s ON s.customer_id = c.id
