select
    customer_id,
    year(start_date) as start_year,
    avg(monthly_fee) as avg_monthly_fee,
    sort_array(collect_set(plan)) as plan_list
from subscriptions
group by customer_id, year(start_date)
having avg_monthly_fee >= 80
order by avg_monthly_fee desc, customer_id, start_year
limit 3
