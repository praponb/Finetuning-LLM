SELECT
    id,
    customer_id,
    items,
    ship_to,
    attributes,
    COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
    EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
    SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines,
    TRANSFORM(
        FILTER(items, item -> item.promo IS NOT NULL),
        item -> STRUCT(item.product_id AS pid, ROUND(item.price * 0.9, 2) AS discounted_price)
    ) AS promo_lines
FROM carts;
