WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, INLINE(items)
    FROM carts
)
-- Multi-level aggregation
SELECT
    CASE WHEN qty >= 5 THEN 'PREMIUM' ELSE 'STANDARD' END AS qty_tier,
    COUNT_IF(promo = 'SPRING10') AS spring10_events,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    SUM(price) AS total_price
FROM exploded
GROUP BY qty_tier
