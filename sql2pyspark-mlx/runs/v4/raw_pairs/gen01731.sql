with with_window as (
    -- Non-equi join on a validity window
    select
        src.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from orders src
    left join promotions pr
        on src.channel = pr.channel
       and src.order_date >= pr.starts_on
       and src.order_date < pr.ends_on + interval 30 days
)
select
    id,
    customer_id,
    order_date,
    amount,
    discount,
    shipping_fee,
    promo_id,
    promo_code
from with_window
order by id nulls first
