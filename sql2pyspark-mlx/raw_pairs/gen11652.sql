with numbered as (
    select
        id,
        customer_id,
        monthly_fee,
        dense_rank() over (partition by customer_id order by monthly_fee desc) rn
    from subscriptions
    where status not in ('CANCELLED', 'PAUSED', 'EXPIRED')
)
select id, customer_id, monthly_fee
from numbered
where rn <= 2
order by id, customer_id
