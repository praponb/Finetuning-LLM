SELECT id, coupon_codes, items, ship_to.country AS ship_country, attributes['device'] AS device, SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines, SIZE(items) AS num_items FROM carts;
