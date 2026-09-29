with ranked as (
    select *, row_number() over (partition by plan order by monthly_fee desc, id) as row_num
    from subscriptions
)
select
    id,
    plan,
    monthly_fee,
    customer_id
from ranked
where row_num <= 1
order by id;
