select *
from (
    select
        id,
        channel,
        shipping_fee,
        amount,
        discount
    from orders
) src
unpivot include nulls (
    reading for measure in (shipping_fee, amount, discount)
) unp;
