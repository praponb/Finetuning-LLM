select c.id, c.email from customers c where lower(trim(c.name)) = 'pim chaiyaporn' and not exists (select 1 from sessions s where s.customer_id = c.id);
