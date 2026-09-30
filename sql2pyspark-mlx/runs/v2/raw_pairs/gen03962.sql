select
    id,
    end_date,
    monthly_fee,
    plan,
    case when status = 'PAUSED' then 1 else 0 end as is_paused
from subscriptions
where monthly_fee >= 180 and (customer_id in (3, 9, 31, 35) or plan = 'premium')
order by monthly_fee desc;
