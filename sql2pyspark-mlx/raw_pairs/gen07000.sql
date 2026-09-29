select
    id,
    coupon_codes,
    line.product_id,
    line.promo
from (
    select
        id,
        coupon_codes,
        explode(items) as line
    from carts
) t;
