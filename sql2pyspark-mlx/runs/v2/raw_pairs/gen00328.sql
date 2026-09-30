select s.id, s.country, s.device, s.customer_id from sessions s where s.country in ('JP', 'DE') and s.customer_id not in (select id from customers where is_active = false)
