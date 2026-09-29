select
    id,
    customer_id,
    created_at,
    coupon_codes,
    pos,
    line.product_id,
    line.qty,
    line.price,
    line.promo
from carts
lateral view outer posexplode(items) li as pos, line
