select
    id,
    status,
    amount,
    dense_rank() over (partition by status order by amount) as dense_rnk
from orders
where shipping_fee < 27
order by id asc;
