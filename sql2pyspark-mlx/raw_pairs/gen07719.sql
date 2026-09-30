WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        ship_to,
        attributes,
        COALESCE(attributes['gift'], 'direct') AS gift_or_default,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        TRANSFORM(
            FILTER(items, i -> i.promo IS NOT NULL),
            i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        EXISTS(items, i -> i.price > 100) AS has_premium_item,
        TRANSFORM(items, i -> i.product_id) AS product_ids
    FROM carts
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    customer_id,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    MIN(created_at) AS first_created_at,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes_2,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    COUNT(*) AS num_rows
FROM parsed
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY num_rows NULLS LAST
