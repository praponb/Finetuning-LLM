select date_format(start_date, 'yyyy-MM') start_month, sum(monthly_fee) total_monthly_fee
from subscriptions
where customer_id is null
group by start_month
having sum(monthly_fee) > 910;
