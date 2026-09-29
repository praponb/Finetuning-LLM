select
    id,
    customer_id,
    created_at,
    coupon_codes,
    k,
    v
from carts
lateral view outer explode(attributes) e as k, v;
