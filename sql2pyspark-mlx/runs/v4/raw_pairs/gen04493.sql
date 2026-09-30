WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(effective_from, 31) AS effective_from_minus_31d,
        YEAR(effective_from) AS effective_from_year
    FROM lines
    WHERE currency = 'EUR'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN effective_from_year >= 79 THEN 'High' WHEN effective_from_year >= 28 THEN 'Medium' ELSE 'Low' END AS effective_from_year_tier,
        CAST(tier_value AS STRING) AS tier_value_str
    FROM cleaned
    WHERE effective_from > '2025-01-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        effective_from_year_tier,
        tier_value_str,
        DATEDIFF(effective_from, effective_from_minus_31d) AS days_effective_from_minus_31d_to_effective_from
    FROM filtered
    WHERE effective_from >= DATE '2024-06-02'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank,
        CUME_DIST() OVER w_part AS days_effective_from_minus_31d_to_effective_from_cume_dist
    FROM base
    WINDOW w_part AS (ORDER BY days_effective_from_minus_31d_to_effective_from NULLS LAST)
),
base_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        currency,
        tier_value,
        effective_from_year_tier,
        tier_value_str,
        days_effective_from_minus_31d_to_effective_from,
        tier_value_rank,
        days_effective_from_minus_31d_to_effective_from_cume_dist,
        LAST_DAY(effective_from) AS effective_from_month_end
    FROM scored
    WHERE days_effective_from_minus_31d_to_effective_from_cume_dist <= 0.44
)
SELECT
    currency,
    effective_from_year_tier,
    tier_value_str,
    days_effective_from_minus_31d_to_effective_from,
    tier_value_rank,
    days_effective_from_minus_31d_to_effective_from_cume_dist,
    effective_from_month_end
FROM base_price_tiers;
