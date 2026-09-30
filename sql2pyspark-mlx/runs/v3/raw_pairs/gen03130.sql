select c.country, c.signup_date, c.credit_limit, s.plan from customers c inner join subscriptions s on s.customer_id = c.id
