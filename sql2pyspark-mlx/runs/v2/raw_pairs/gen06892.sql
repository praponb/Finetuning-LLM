select s.end_date, c.email, c.signup_date, c.is_active, length(c.country) country_length from subscriptions s inner join customers c on s.customer_id = c.id
