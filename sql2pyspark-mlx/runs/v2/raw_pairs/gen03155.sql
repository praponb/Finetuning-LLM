select status, round(avg(monthly_fee), 2) as avg_monthly_fee, concat_ws(', ', sort_array(collect_set(plan))) as plan_values
from subscriptions
where customer_id is null and (monthly_fee is not null or customer_id in (10, 22, 29))
group by status;
