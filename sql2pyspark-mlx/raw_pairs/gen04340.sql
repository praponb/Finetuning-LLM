SELECT
    id,
    created_at,
    items,
    ship_to,
    TRANSFORM(
        FILTER(items, li -> li.promo IS NOT NULL),
        li -> STRUCT(li.product_id AS pid, ROUND(li.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines,
    ship_to.zip AS ship_zip,
    EXISTS(items, li -> li.price > 100) AS has_premium_item,
    ship_to.country AS ship_country
FROM carts
WHERE created_at < '2023-05-31';
