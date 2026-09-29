select *, round(monthly_fee * 100, 2) as monthly_fee_adjusted
from subscriptions
where monthly_fee >= 140
order by customer_id desc
