select
    id,
    coupon_codes,
    pos,
    line.product_id,
    line.qty
from carts
lateral view outer posexplode(items) x as pos, line;
