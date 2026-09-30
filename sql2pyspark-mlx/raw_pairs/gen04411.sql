WITH flattened AS (SELECT id, created_at, coupon_codes, INLINE(items) FROM carts) SELECT id, created_at, coupon_codes, price, promo FROM flattened ORDER BY created_at DESC, promo
