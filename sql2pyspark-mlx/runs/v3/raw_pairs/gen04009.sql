select end_month, unique_statuss, premium_count
from (
    select month(end_date) end_month, count(distinct monthly_fee) unique_statuss, count(case when plan = 'premium' then 1 end) premium_count
    from subscriptions
    where start_date >= '2025-05-11'
    group by month(end_date)
) s
where premium_count >= 6
order by premium_count desc;
