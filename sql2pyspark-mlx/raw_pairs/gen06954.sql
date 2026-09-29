with flattened as (
    select
        id,
        customer_id,
        created_at,
        value
    from carts
    lateral view explode(coupon_codes) li as value
)
select
    id,
    customer_id,
    created_at,
    value
from flattened
order by value desc
