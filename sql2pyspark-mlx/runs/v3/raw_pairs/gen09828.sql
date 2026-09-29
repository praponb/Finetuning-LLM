select
    customer_id,
    case when monthly_fee >= 220 then 'high' when monthly_fee >= 47 then 'mid' else 'low' end monthly_fee_bucket,
    case when status in ('CANCELLED', 'PAUSED') then 'tier_1' else 'other' end status_group,
    avg(monthly_fee) avg_monthly_fee
from subscriptions
group by customer_id, case when monthly_fee >= 220 then 'high' when monthly_fee >= 47 then 'mid' else 'low' end, case when status in ('CANCELLED', 'PAUSED') then 'tier_1' else 'other' end
having avg(monthly_fee) >= 150
order by avg_monthly_fee, monthly_fee_bucket
