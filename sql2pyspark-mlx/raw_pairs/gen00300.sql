WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        items,
        ship_to,
        SIZE(FILTER(items, x -> x.qty >= 3)) AS bulk_lines,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        attributes['gift'] AS gift
    FROM carts
)
-- Grouped metrics
SELECT
    CASE WHEN bulk_lines >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END AS bulk_lines_tier,
    PERCENTILE_APPROX(bulk_lines, ARRAY(0.25, 0.5, 0.75)) AS bulk_lines_quartiles,
    COUNT(*) AS cnt,
    COUNT(*) AS event_count
FROM unpacked
GROUP BY CASE WHEN bulk_lines >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING cnt > 1
ORDER BY bulk_lines_tier
