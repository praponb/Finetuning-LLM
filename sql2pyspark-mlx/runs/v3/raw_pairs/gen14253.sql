select
    c.id,
    c.is_active,
    c.email,
    t.last_started_at
from customers c
left join (select customer_id, max(started_at) last_started_at
    from sessions
    group by customer_id) t on c.id = t.customer_id;
