select
    case when monthly_fee >= 221 then 'high' when monthly_fee >= 108 then 'mid' else 'low' end as monthly_fee_bucket,
    status,
    case when year(end_date) < 2024 then 'before_2024' when year(end_date) = 2024 then '2024' else 'after_2024' end as end_cohort,
    max(start_date) as last_start_date
from subscriptions
group by 1, 2, 3;
