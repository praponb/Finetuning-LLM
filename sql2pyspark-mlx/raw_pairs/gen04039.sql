SELECT id, created_at, coupon_codes, items, attributes, ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text, TRANSFORM(items, i -> i.product_id) AS product_ids FROM carts
