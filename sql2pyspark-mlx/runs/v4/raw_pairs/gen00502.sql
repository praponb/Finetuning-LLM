with exploded as (
    select
        id,
        customer_id,
        code
    from carts
    lateral view outer explode(coupon_codes) li as code
)
select
    customer_id,
    count(distinct code) as distinct_codes,
    count(*) filter (where code = 'VIP') as vip_rows,
    count(*) filter (where code = 'WELCOME5') as welcome5_rows
from exploded
group by customer_id
order by vip_rows
