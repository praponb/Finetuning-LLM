with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        tag
    from carts
    lateral view explode(coupon_codes) x as tag
)
select
    id,
    created_at,
    tag
from exploded
order by created_at nulls last
