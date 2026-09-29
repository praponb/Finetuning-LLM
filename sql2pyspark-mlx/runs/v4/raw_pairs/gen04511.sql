select *
from (
    select
        id,
        customer_id,
        shipping_fee,
        amount,
        discount
    from orders
) src
unpivot (
    reading for metric_name in (shipping_fee, amount, discount)
);
