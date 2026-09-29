SELECT s.id, s.device, c.name, c.city, c.credit_limit, DATE_ADD(c.signup_date, 6) AS signup_date_plus_6d FROM sessions s JOIN customers c ON c.id = s.customer_id
