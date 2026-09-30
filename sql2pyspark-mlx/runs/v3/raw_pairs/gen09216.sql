select
    id,
    order_id,
    paid_at,
    method
from (
    select *, row_number() over (partition by order_id order by paid_at, id) rnk
    from payments
    where order_id in (16, 18, 30, 32)
) t
where rnk <= 1;
