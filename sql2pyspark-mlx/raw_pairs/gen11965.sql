select
    id,
    store_id,
    metric_name,
    value
from (
    select
        id,
        store_id,
        discount,
        amount,
        shipping_fee
    from orders
) src
unpivot include nulls (
    value for metric_name in (discount as DISCOUNT, amount as AMOUNT, shipping_fee)
) u;
