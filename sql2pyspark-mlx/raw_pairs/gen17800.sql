select id, customer_id, created_at, coupon_codes, k, attr_value from carts lateral view explode(attributes) t as k, attr_value;
