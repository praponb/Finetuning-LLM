select s.browser, s.country, s.started_at, c.credit_limit, c.segment from sessions s inner join customers c on s.customer_id = c.id where c.segment != 'Corporate' order by c.credit_limit desc;
