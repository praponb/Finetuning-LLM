select
    base.id,
    base.customer_id,
    base.coupon_codes,
    entry.qty,
    entry.price,
    entry.promo
from carts base
lateral view outer explode(base.items) t as entry
