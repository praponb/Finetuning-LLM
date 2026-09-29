select c.country, max(s.started_at) as latest_started_at
from sessions s
left outer join customers c on s.customer_id = c.id
where s.customer_id in (10, 21, 32)
group by c.country;
