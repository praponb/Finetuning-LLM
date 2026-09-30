WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(val, 2) val_rounded,
        CASE WHEN currency = 'EUR' THEN 1 ELSE 0 END is_eur
    FROM flattened
    WHERE effective_from > DATE '2024-02-13'
      AND val > 34
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        currency,
        val,
        val_rounded,
        is_eur,
        is_eur / NULLIF(val_rounded, 0) is_eur_per_val_rounded
    FROM filtered
    WHERE effective_from >= DATE '2024-01-27'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(val_rounded) OVER (PARTITION BY currency) currency_count_val_rounded,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val NULLS FIRST) val_percent_rank,
        COUNT(val_rounded) OVER (PARTITION BY DATE_TRUNC('WEEK', effective_from)) effective_from_period_count_val_rounded,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', effective_from), currency ORDER BY is_eur DESC NULLS FIRST) is_eur_cume_dist
    FROM cleaned
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        effective_from,
        currency,
        val,
        val_rounded,
        is_eur,
        is_eur_per_val_rounded,
        effective_from_period_count_val_rounded,
        CASE WHEN is_eur_cume_dist >= 0.7 THEN 'High' WHEN is_eur_cume_dist >= 0.24 THEN 'Medium' ELSE 'Low' END is_eur_cume_dist_band
    FROM scored
    WHERE effective_from > TO_DATE('2024-03-31')
),
filtered_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(effective_from_period_count_val_rounded, 0) effective_from_period_count_val_rounded_filled
    FROM base
    WHERE effective_from_period_count_val_rounded BETWEEN 11 AND 16
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(currency) currency_lower,
        INITCAP(TRIM(currency)) currency_title
    FROM filtered_price_tiers
    WHERE effective_from > DATE '2024-02-03'
)
SELECT
    currency,
    val,
    is_eur,
    is_eur_per_val_rounded,
    is_eur_cume_dist_band,
    effective_from_period_count_val_rounded_filled,
    currency_lower,
    currency_title,
    CASE WHEN currency RLIKE '^(USD|EUR)$' THEN 'OTHER_ZONE' WHEN currency RLIKE '^(THB)$' THEN 'CORE' ELSE 'GROUP_B' END AS currency_zone
FROM valid
ORDER BY currency_zone DESC
