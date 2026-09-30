with ranked as (
    select
        id,
        plan,
        monthly_fee,
        row_number() over (partition by plan order by monthly_fee desc, id) as pos
    from subscriptions
)
select id, plan, monthly_fee
from ranked
where pos <= 2
order by monthly_fee
