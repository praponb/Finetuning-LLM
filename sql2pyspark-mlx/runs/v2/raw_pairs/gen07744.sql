select
    id,
    status,
    amount,
    store_id,
    cast(amount as int) as amount_int
from orders
where status in ('RETURNED', 'CANCELLED', 'SHIPPED') and channel = 'partner'
order by id desc, amount
