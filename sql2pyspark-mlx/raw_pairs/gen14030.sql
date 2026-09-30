select
    c.id,
    c.created_at,
    pos + 1 as line_no,
    elem.product_id,
    elem.qty,
    elem.price
from carts c
lateral view outer posexplode(c.items) t as pos, elem
