select
    id,
    status,
    amount,
    round(amount / sum(amount) over (partition by status), 3) as amount_share
from orders
where customer_id in (4, 24);
