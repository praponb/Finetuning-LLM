select id, tag
from carts
lateral view outer explode(coupon_codes) x as tag;
