select
    id,
    order_id,
    amount,
    dense_rank() over (partition by order_id order by amount) as amount_dense_rank
from payments
where amount <= 1020;
