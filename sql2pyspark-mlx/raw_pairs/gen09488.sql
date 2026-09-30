select *, coalesce(monthly_fee, 0) monthly_fee_filled, coalesce(monthly_fee, 0) monthly_fee_filled_2
from subscriptions
where monthly_fee between 80 and 150
order by customer_id;
