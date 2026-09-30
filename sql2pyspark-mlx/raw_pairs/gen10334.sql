WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        created_at,
        coupon_codes,
        ship_to,
        ship_to.zip AS ship_zip,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        COALESCE(attributes['gift'], 'direct') AS gift_or_default,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS items_total,
        EXISTS(items, item -> item.price > 100) AS has_premium_item
    FROM carts
    WHERE SIZE(coupon_codes) > 2
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN items_total >= 1560 THEN 'HIGH' ELSE 'LOW' END AS items_total_tier,
    COUNT(*) AS cnt,
    MAX(created_at) AS ended_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    COUNT(*) AS num_rows
FROM parsed
GROUP BY DATE_TRUNC('DAY', created_at), CASE WHEN items_total >= 1560 THEN 'HIGH' ELSE 'LOW' END
HAVING num_rows >= 1;
