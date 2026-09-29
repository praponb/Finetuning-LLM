select *
from (
    select plan, min(start_date) as min_start_date, max(monthly_fee) - min(monthly_fee) as monthly_fee_range
    from subscriptions
    group by plan
) as t
where monthly_fee_range >= 36
