select *
from (
    select
        id,
        channel,
        discount,
        amount,
        shipping_fee
    from orders
) src
unpivot (
    val for metric in (discount as DISCOUNT_VAL, amount as AMOUNT, shipping_fee)
)
