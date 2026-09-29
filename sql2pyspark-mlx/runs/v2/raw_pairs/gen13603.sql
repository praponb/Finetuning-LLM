with customer_summary as (
    select customer_id, min(end_date) as min_end_date
    from subscriptions
    group by customer_id
)
select
    c.id,
    c.city,
    c.signup_date,
    cs.min_end_date
from customers c
inner join customer_summary cs on c.id = cs.customer_id;
