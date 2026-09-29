select
    id,
    customer_id,
    start_date,
    plan
from (
    select *, row_number() over (partition by customer_id order by start_date desc, id) row_num
    from subscriptions
    where monthly_fee > 180
) t
where row_num <= 1;
