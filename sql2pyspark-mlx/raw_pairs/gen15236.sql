select id, plan, monthly_fee
from (
    select *, row_number() over (partition by plan order by monthly_fee desc, id) as rnk
    from subscriptions
) as r
where rnk = 1
