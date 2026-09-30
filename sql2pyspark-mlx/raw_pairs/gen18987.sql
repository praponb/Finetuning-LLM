select
    c.id,
    c.created_at,
    c.coupon_codes,
    pos,
    item.product_id,
    item.price,
    item.promo
from carts c
lateral view outer posexplode(c.items) e as pos, item;
