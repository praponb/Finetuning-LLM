select
    id,
    end_date,
    status,
    customer_id,
    concat(status, ' ', plan) as status_plan
from subscriptions
where customer_id is not null and status in ('ACTIVE', 'PAUSED');
