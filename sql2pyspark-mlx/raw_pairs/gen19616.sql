WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    COUNT(DISTINCT plan) AS distinct_plans,
    PERCENTILE_APPROX(element, 0.95) AS element_p95,
    COUNT(*) AS row_count
FROM lines
GROUP BY DATE_TRUNC('YEAR', effective_from)
ORDER BY distinct_plans DESC NULLS LAST;
