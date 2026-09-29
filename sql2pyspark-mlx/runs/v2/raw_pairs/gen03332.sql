select
    case when monthly_fee >= 221 then 'high' when monthly_fee >= 77 then 'mid' else 'low' end as monthly_fee_bucket,
    plan,
    max(end_date) as latest_end_date,
    min(start_date) as min_start_date
from subscriptions
where customer_id in (8, 10, 12, 28) and end_date >= '2023-05-01'
group by case when monthly_fee >= 221 then 'high' when monthly_fee >= 77 then 'mid' else 'low' end, plan;
