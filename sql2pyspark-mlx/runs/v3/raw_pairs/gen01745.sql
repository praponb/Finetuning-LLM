select c.email, a.opened_date, date_trunc('week', c.signup_date) as signup_date_period from customers c left outer join accounts a on c.id = a.customer_id where c.segment = 'SMB'
