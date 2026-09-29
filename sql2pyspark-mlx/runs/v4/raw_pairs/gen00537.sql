WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        ship_to,
        attributes,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty,
        ship_to.zip AS ship_zip,
        TRANSFORM(
            FILTER(items, item -> item.promo IS NOT NULL),
            item -> STRUCT(item.product_id AS pid, ROUND(item.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code
    FROM carts
    WHERE HOUR(created_at) BETWEEN 11 AND 17
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(DISTINCT country_or_unknown) AS distinct_country_or_unknowns,
    MAX(created_at) AS last_seen,
    MAX(created_at) AS last_seen_2,
    MIN(created_at) AS first_seen,
    GROUPING_ID() AS gid
FROM flattened_attrs
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), DATE_TRUNC('HOUR', created_at)), (DATE_TRUNC('DAY', created_at)), ())
ORDER BY hour_window DESC
