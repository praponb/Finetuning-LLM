select s.id, s.started_at, s.device from sessions s where s.started_at >= '2024-05-01' and s.customer_id in (select id from customers where country = 'SG') order by s.device asc
