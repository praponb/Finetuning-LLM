SELECT id, customer_id, created_at, coupon_codes, COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown, ship_to.city AS ship_city FROM carts WHERE SIZE(coupon_codes) > 1;
