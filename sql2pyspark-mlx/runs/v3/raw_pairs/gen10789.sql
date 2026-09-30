select start_month, active_count, total_monthly_fee
from (
    select date_format(start_date, 'yyyy-MM') as start_month, sum(case when status = 'ACTIVE' then 1 else 0 end) as active_count, sum(monthly_fee) as total_monthly_fee
    from subscriptions
    where plan not in ('standard', 'enterprise', 'premium')
    group by date_format(start_date, 'yyyy-MM')
) as x
where total_monthly_fee >= 600
order by 2 desc
