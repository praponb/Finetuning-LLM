with ranked as (
    select
        id,
        customer_id,
        monthly_fee,
        plan,
        dense_rank() over (partition by customer_id order by monthly_fee desc) as rnk
    from subscriptions
)
select
    id,
    customer_id,
    monthly_fee,
    plan
from ranked
where rnk <= 3
order by plan desc
