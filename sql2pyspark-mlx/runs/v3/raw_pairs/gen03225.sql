with ranked as (
    select
        id,
        status,
        amount,
        channel,
        row_number() over (partition by status order by amount desc, id) as rnk
    from orders
    where order_date between '2025-04-01' and '2025-05-20'
)
select
    id,
    status,
    amount,
    channel
from ranked
where rnk <= 1
