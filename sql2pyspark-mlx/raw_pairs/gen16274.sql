WITH flattened_attrs AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        items,
        ARRAY_MAX(TRANSFORM(items, i -> i.price)) AS max_item_price,
        SIZE(FILTER(items, i -> i.qty >= 3)) AS bulk_lines,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code
    FROM carts
)
SELECT
    id,
    created_at,
    max_item_price,
    LAST_VALUE(max_item_price) IGNORE NULLS OVER w_running AS last_known_max_item_price,
    SUM(max_item_price) OVER w_running AS cumulative_max_item_price
FROM flattened_attrs
WINDOW w_running AS (ORDER BY bulk_lines, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
