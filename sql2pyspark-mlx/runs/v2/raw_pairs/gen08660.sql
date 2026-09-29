select s.monthly_fee, c.signup_date from subscriptions s inner join customers c on s.customer_id = c.id order by monthly_fee desc
