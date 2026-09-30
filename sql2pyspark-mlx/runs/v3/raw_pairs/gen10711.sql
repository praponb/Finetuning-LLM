select id, customer_id, monthly_fee
from (
    select
        id,
        customer_id,
        monthly_fee,
        rank() over (partition by customer_id order by monthly_fee desc) as rnk
    from subscriptions
) as t
where rnk <= 5
