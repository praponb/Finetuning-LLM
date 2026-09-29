select
    id,
    customer_id,
    created_at,
    code
from carts
lateral view explode(coupon_codes) x as code;
