select
    c.city,
    c.is_active,
    count(distinct s.country) as unique_countrys,
    min(s.started_at) as first_started_at
from sessions s
full outer join customers c on s.customer_id = c.id
group by c.city, c.is_active
order by first_started_at, 1 desc;
