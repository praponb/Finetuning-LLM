SELECT id, created_at, items, attributes, SIZE(FILTER(items, x -> x.qty >= 3)) AS bulk_lines, ship_to.country AS ship_country, SIZE(items) AS num_items FROM carts;
