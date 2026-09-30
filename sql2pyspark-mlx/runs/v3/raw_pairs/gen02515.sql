select s.id, s.device, s.started_at, s.browser from sessions s where to_date(s.started_at) >= '2025-07-02' and s.customer_id not in (select id from customers where name like 'K%')
