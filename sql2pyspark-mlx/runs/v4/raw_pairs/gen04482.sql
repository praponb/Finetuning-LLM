WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        created_at,
        items,
        attributes,
        TRANSFORM(
            FILTER(items, item -> item.promo IS NOT NULL),
            item -> STRUCT(item.product_id AS pid, ROUND(item.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
        COALESCE(attributes['device'], 'none') AS device_or_default
    FROM carts
)
-- Grouped metrics
SELECT
    customer_id,
    MAX(created_at) AS last_created_at,
    COUNT(*) AS row_count,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    COUNT(*) AS row_count_2,
    MAX(created_at) AS last_created_at_2,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM parsed
GROUP BY customer_id
ORDER BY duration_min DESC
