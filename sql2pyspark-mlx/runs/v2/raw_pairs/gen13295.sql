select c.id, c.city, c.segment, c.email from customers c where exists (select 1 from sessions s where s.customer_id = c.id and s.device = 'mobile');
