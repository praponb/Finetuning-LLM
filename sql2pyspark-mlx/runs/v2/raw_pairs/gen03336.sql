select a.customer_id, c.is_active, c.email, c.signup_date from accounts a inner join customers c on c.id = a.customer_id where month(c.signup_date) in (4, 8, 12)
