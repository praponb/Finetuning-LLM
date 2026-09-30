select
    id,
    paid_at,
    cast(amount as int) as amount_int,
    case when amount >= 1290 then 'High' when amount >= 730 then 'Medium' else 'Low' end as amount_tier
from payments
where status != 'FAILED' or amount > 950
