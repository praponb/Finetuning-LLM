select s.id, s.customer_id, s.device, s.started_at from sessions s where s.customer_id in (select id from customers where segment = 'Consumer');
