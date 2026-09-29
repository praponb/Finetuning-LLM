select *
from (
    select
        customer_id,
        store_id,
        status,
        amount
    from orders
) src
pivot (
    avg(amount)
    for status in ('CANCELLED' as cancelled, 'SHIPPED' as shipped)
)
