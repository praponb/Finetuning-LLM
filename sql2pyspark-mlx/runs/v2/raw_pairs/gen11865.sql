with CustomerStats as (
    select customer_id, max(end_date) as last_end_date
    from subscriptions
    group by customer_id
)
select c.is_active, C.last_end_date
from customers c
left join CustomerStats C on c.id = C.customer_id
order by last_end_date;
