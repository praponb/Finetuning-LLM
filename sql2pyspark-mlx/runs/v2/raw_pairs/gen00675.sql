select s.id, s.end_date, s.monthly_fee, c.email, c.signup_date, c.country from subscriptions s left join customers c on s.customer_id = c.id order by s.monthly_fee, id limit 10
