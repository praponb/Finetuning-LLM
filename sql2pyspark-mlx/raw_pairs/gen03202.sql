WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(effective_from) effective_from_month_end
    FROM flattened
    WHERE val <= 67
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        effective_from,
        val,
        effective_from_month_end,
        WEEKOFYEAR(effective_from) effective_from_weekofyear
    FROM base
    WHERE val > 63
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        val,
        effective_from_month_end,
        effective_from_weekofyear,
        COALESCE(val, 0) val_filled,
        CAST(effective_from_weekofyear AS DOUBLE) effective_from_weekofyear_str
    FROM valid
    WHERE YEAR(effective_from_month_end) >= 2025
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', effective_from_month_end) AS year_start,
    CASE WHEN effective_from_weekofyear >= 89 THEN 'LARGE' ELSE 'SMALL' END AS effective_from_weekofyear_tier,
    MIN(val) AS min_val,
    ROUND(AVG(effective_from_weekofyear_str), 2) AS avg_effective_from_weekofyear_str,
    PERCENTILE_APPROX(effective_from_weekofyear_str, 0.95) AS effective_from_weekofyear_str_p95
FROM filtered
GROUP BY year_start, effective_from_weekofyear_tier
ORDER BY effective_from_weekofyear_str_p95 NULLS FIRST;
