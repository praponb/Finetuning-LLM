select
    id,
    store_id,
    order_date,
    status,
    amount * discount as amount_times_discount
from orders
order by 1 asc
limit 1;
