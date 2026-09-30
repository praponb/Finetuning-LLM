select
    method,
    order_id,
    status,
    floor(amount) as amount_whole
from payments
order by order_id desc;
