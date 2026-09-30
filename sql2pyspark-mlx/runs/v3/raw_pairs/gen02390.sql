select c.id, count(s.id) as session_count, min(s.started_at) as min_started_at from customers c left outer join sessions s on s.customer_id = c.id group by c.id order by session_count desc
