select s.id, s.browser, s.customer_id, c.credit_limit, c.segment, c.is_active from sessions s join customers c on s.customer_id = c.id order by id, browser;
