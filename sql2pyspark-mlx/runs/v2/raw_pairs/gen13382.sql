select c.name, t.min_started_at
from customers c
left outer join (select customer_id, min(started_at) as min_started_at
    from sessions
    group by customer_id) t on c.id = t.customer_id
order by min_started_at;
