select s.id, s.country, s.customer_id, s.device from sessions s where s.customer_id in (select id from customers where signup_date > '2025-03-01');
