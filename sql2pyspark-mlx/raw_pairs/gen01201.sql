select id, created_at, value from carts lateral view explode(coupon_codes) t as value;
