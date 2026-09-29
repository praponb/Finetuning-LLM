select
    id,
    customer_id,
    monthly_fee,
    status
from (
    select *, row_number() over (partition by customer_id order by monthly_fee, id) as pos
    from subscriptions
) as t
where pos <= 2
order by monthly_fee, id desc;
