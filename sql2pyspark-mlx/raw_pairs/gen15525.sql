select id, customer_id, created_at, coupon_codes, entry.price, entry.promo from carts lateral view explode(items) li as entry;
