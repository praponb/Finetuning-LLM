select c.is_active, min(s.started_at) as min_started_at
from sessions s
left join customers c on s.customer_id = c.id
where s.customer_id is not null
group by c.is_active
