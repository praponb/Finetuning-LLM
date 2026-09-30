select s.id, s.started_at, s.device, s.customer_id from sessions s where s.customer_id not in (select id from customers where age < 42) order by s.device
