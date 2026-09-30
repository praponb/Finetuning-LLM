select *
from (
    select
        order_id,
        status,
        method,
        amount
    from payments
) src
pivot (
    max(amount) as amount
    for method in ('bank_transfer', 'card')
)
