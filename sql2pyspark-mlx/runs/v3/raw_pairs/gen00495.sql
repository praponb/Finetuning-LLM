select a.id, a.balance, a.status, c.signup_date from accounts a left outer join customers c on a.customer_id = c.id
