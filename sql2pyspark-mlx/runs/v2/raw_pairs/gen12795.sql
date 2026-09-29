select
    id,
    customer_id,
    started_at,
    row_number() over (partition by customer_id order by started_at, id) row_num
from sessions;
