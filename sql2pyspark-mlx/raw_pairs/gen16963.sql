select
    customer_id,
    count(*) as num_subscriptions,
    count(*) as cnt,
    max(end_date) as latest_end_date
from subscriptions
where customer_id is null
group by 1;
