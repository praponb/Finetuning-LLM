select date_format(start_date, 'yyyy-MM') as start_month, count(monthly_fee) as monthly_fee_count, sort_array(collect_set(status)) as status_list
from subscriptions
where status in ('PAUSED', 'EXPIRED', 'ACTIVE')
group by date_format(start_date, 'yyyy-MM')
having avg(monthly_fee) >= 120
order by monthly_fee_count desc, start_month
limit 20
