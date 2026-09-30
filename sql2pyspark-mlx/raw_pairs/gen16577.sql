select
    c.id,
    c.created_at,
    pos + 1 as position,
    line.product_id,
    line.price,
    line.promo
from carts c
lateral view outer posexplode(c.items) li as pos, line
