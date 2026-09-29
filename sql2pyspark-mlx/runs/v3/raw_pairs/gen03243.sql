select s.customer_id, s.started_at, s.browser, c.name, c.email from sessions s left join customers c on s.customer_id = c.id where s.country = 'UK' order by s.customer_id
