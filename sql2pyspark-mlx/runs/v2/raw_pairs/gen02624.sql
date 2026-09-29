select
    id,
    end_date,
    status,
    customer_id
from subscriptions
where end_date > (select max(end_date) from subscriptions)
order by status asc
