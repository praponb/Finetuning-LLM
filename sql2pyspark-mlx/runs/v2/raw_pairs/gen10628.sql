select customer_id, sum(case when status = 'CANCELLED' then monthly_fee else 0 end) as cancelled_monthly_fee
from subscriptions
where customer_id is not null
group by customer_id;
