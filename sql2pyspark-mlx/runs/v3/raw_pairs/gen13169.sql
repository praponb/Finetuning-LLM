with customer_stats as (
    select customer_id, max(started_at) as latest_started_at
    from sessions
    group by customer_id
)
select
    c.id,
    c.city,
    c.is_active,
    cs.latest_started_at
from customers c
inner join customer_stats cs on c.id = cs.customer_id;
