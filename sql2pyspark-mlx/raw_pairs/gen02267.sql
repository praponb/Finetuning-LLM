with customer_summary as (
    select customer_id, min(started_at) as min_started_at
    from sessions
    group by customer_id
)
select c.country, cs.min_started_at
from customers c
inner join customer_summary cs on c.id = cs.customer_id
order by min_started_at, country desc;
