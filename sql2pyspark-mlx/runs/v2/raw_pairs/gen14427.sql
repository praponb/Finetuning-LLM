select
    id,
    order_id,
    amount,
    round(amount / sum(amount) over (partition by order_id), 3) as amount_share
from payments;
