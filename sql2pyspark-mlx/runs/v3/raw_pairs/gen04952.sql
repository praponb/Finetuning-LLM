select s.id, s.browser from sessions s where s.customer_id in (select id from customers where name is not null)
