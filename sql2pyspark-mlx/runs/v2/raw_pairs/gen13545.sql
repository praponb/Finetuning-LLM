select
    id,
    device,
    started_at,
    customer_id,
    first_value(customer_id) over (partition by device order by started_at, id) as first_customer_id
from sessions;
