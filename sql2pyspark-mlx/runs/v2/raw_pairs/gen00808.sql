with sessions_by_customer as (
    select customer_id, min(started_at) as min_started_at
    from sessions
    where to_date(started_at) >= '2025-01-01'
    group by customer_id
)
select
    c.id,
    c.country,
    c.name,
    sbc.min_started_at
from customers c
left join sessions_by_customer sbc on c.id = sbc.customer_id;
