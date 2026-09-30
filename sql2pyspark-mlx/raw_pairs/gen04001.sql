select quarter(c.signup_date) as signup_quarter, sort_array(collect_set(s.browser)) as browser_list from sessions s join customers c on s.customer_id = c.id group by quarter(c.signup_date);
