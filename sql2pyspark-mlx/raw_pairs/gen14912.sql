select a.id, a.customer_id, a.status, c.signup_date, c.city from accounts a join customers c on a.customer_id = c.id
