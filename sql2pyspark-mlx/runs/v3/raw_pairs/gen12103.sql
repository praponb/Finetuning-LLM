select
    id,
    store_id,
    amount,
    customer_id,
    row_number() over (partition by store_id order by amount desc, id) as row_num,
    round(amount / sum(amount) over (partition by customer_id), 3) as amount_share
from orders
where customer_id in (4, 30, 38);
