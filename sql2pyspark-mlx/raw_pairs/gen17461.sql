WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    CASE WHEN element >= 68 THEN 'LARGE' ELSE 'SMALL' END AS element_tier,
    APPROX_PERCENTILE(element, ARRAY(0.25, 0.5, 0.75)) AS element_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows
FROM exploded
GROUP BY CASE WHEN element >= 68 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) >= 1
ORDER BY distinct_ids DESC NULLS FIRST;
