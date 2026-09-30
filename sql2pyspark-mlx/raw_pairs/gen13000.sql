with filtered_subscriptions as (
    select *
    from subscriptions
    where plan = 'premium' and status = 'PAUSED'
),
summary as (
    select date_format(start_date, 'yyyy-MM') as start_month, case when plan in ('basic', 'standard') then 'priority' else 'other' end as plan_group, round(stddev(monthly_fee), 2) as stddev_monthly_fee
    from filtered_subscriptions
    group by date_format(start_date, 'yyyy-MM'), case when plan in ('basic', 'standard') then 'priority' else 'other' end
)
select start_month, stddev_monthly_fee
from summary
where stddev_monthly_fee >= 63
order by stddev_monthly_fee desc
