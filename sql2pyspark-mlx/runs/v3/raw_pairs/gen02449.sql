select *
from (
    select
        id,
        status,
        monthly_fee,
        customer_id,
        rank() over (partition by status order by monthly_fee desc) as pos
    from subscriptions
) as t
where pos <= 5
order by id;
