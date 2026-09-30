select *
from (
    select
        order_id,
        method,
        amount
    from payments
) src
pivot (
    max(amount)
    for method in ('bank_transfer', 'card')
);
