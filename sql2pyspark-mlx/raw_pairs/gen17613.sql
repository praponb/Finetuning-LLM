select
    src.id,
    src.customer_id,
    src.coupon_codes,
    li_item.price,
    li_item.promo
from carts src
lateral view explode(src.items) e as li_item;
