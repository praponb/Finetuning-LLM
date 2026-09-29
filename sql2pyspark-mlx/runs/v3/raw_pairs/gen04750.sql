select c.is_active, count(*) as session_count from sessions s full outer join customers c on c.id = s.customer_id group by c.is_active order by session_count desc;
