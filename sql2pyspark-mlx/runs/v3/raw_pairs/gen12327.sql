select
    id,
    order_id,
    paid_at,
    row_number() over (partition by order_id order by paid_at, id) as rn
from payments;
