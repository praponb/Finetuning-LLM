select
    c.is_active,
    c.city,
    min(s.started_at) as min_started_at,
    max(s.started_at) as latest_started_at
from sessions s
join customers c on s.customer_id = c.id
group by c.is_active, c.city
order by latest_started_at asc
