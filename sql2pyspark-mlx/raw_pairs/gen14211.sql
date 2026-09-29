select
    id,
    device,
    started_at
from sessions
qualify row_number() over (partition by customer_id order by started_at desc, id) = 1;
