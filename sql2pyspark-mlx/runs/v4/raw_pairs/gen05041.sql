WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        items,
        ship_to,
        COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
        TRANSFORM(
            FILTER(items, x -> x.promo IS NOT NULL),
            x -> STRUCT(x.product_id AS pid, ROUND(x.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        ship_to.zip AS ship_zip
    FROM carts
    WHERE customer_id = 8
)
-- Multi-level aggregation
SELECT
    CASE WHEN max_item_price >= 189 THEN 'HIGH' ELSE 'LOW' END AS max_item_price_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    APPROX_PERCENTILE(max_item_price, ARRAY(0.25, 0.5, 0.75)) AS max_item_price_quartiles,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    MAX(max_item_price) AS max_max_item_price,
    GROUPING_ID() AS grouping_level
FROM flattened_attrs
GROUP BY GROUPING SETS ((CASE WHEN max_item_price >= 189 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('HOUR', created_at)), (CASE WHEN max_item_price >= 189 THEN 'HIGH' ELSE 'LOW' END))
HAVING max_max_item_price > 0
ORDER BY max_max_item_price NULLS LAST;
