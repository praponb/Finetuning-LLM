with enriched as (
    select
        *,
        last_value(amount) ignore nulls over (order by order_date, id rows between unbounded preceding and current row) as last_known_amount,
        corr(amount, discount) over w_recent as corr_amount_discount
    from orders
    window w_recent as (partition by channel)
)
select
    id,
    customer_id,
    order_date,
    amount,
    discount,
    shipping_fee,
    last_known_amount,
    corr_amount_discount
from enriched
order by shipping_fee desc nulls first
