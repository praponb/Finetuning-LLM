with ranked as (
    select
        id,
        order_id,
        amount,
        status,
        row_number() over (partition by order_id order by amount desc, id) as rnk
    from payments
)
select
    id,
    order_id,
    amount,
    status
from ranked
where rnk <= 1
order by status;
