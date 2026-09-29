select
    id,
    monthly_fee,
    customer_id,
    start_date
from subscriptions
where monthly_fee >= (select max(monthly_fee) from subscriptions where customer_id is not null)
order by 4
