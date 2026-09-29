with src as (
    select *, trim(plan) as clean_plan
    from subscriptions
    where plan = 'basic'
),
agg as (
    select case when monthly_fee >= 260 then 'high' when monthly_fee >= 89 then 'mid' else 'low' end as monthly_fee_bucket, count(case when status = 'CANCELLED' then 1 end) as cancelled_count, round(avg(monthly_fee), 2) as avg_monthly_fee
    from src
    group by case when monthly_fee >= 260 then 'high' when monthly_fee >= 89 then 'mid' else 'low' end
)
select monthly_fee_bucket, cancelled_count, avg_monthly_fee
from agg
where avg_monthly_fee >= 60
order by cancelled_count, monthly_fee_bucket
limit 3;
