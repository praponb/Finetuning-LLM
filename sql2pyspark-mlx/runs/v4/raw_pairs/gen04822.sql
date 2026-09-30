select
    id,
    plan,
    start_date,
    case when monthly_fee >= 223 then 'High' when monthly_fee >= 81 then 'Medium' else 'Low' end as monthly_fee_band
from subscriptions
order by id desc
limit 5;
