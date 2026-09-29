select year(c.signup_date) as signup_year, c.segment, max(a.opened_date) as last_opened_date from accounts a right join customers c on a.customer_id = c.id group by year(c.signup_date), c.segment;
