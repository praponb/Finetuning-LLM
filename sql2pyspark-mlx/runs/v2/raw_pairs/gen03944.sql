select status, max_monthly_fee, pct_cancelled
from (
    select status, max(monthly_fee) as max_monthly_fee, round((100 * sum(case when status = 'CANCELLED' then 1 else 0 end)) / count(*), 1) as pct_cancelled
    from subscriptions
    group by status
) as s
where pct_cancelled < 60
order by max_monthly_fee desc
