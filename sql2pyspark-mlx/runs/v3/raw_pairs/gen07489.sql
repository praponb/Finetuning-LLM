select *
from (
    select
        plan,
        month(start_date) as start_month,
        sum(case when status = 'EXPIRED' then 1 else 0 end) as expired_count,
        count(case when plan = 'premium' then 1 end) as premium_count
    from subscriptions
    group by plan, month(start_date)
) as x
where premium_count >= 9
order by plan;
