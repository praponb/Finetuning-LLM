select
    status,
    round(stddev(monthly_fee), 2) as stddev_monthly_fee,
    sum(case when plan = 'standard' then monthly_fee else 0 end) as standard_monthly_fee,
    max(monthly_fee) - min(monthly_fee) as monthly_fee_range
from subscriptions
where status != 'PAUSED' or monthly_fee >= 180
group by status
order by monthly_fee_range desc;
