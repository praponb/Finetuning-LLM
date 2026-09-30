SELECT
    id,
    SIZE(items) AS num_items,
    TRANSFORM(
        FILTER(items, li -> li.promo IS NOT NULL),
        li -> STRUCT(li.product_id AS pid, ROUND(li.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines
FROM carts;
