with lines as (
    select id, code
    from carts
    lateral view outer explode(coupon_codes) li as code
)
select id, code
from lines
order by code nulls first
