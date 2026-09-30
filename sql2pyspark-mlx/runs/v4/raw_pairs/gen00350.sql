SELECT
    id,
    customer_id,
    created_at,
    items,
    ship_to,
    ship_to.country AS ship_country,
    SIZE(FILTER(items, li -> li.qty >= 3)) AS bulk_lines
FROM carts
