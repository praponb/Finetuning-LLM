WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist
    FROM flattened
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, val_cume_dist + val AS val_cume_dist_plus_val
    FROM scored
    WHERE effective_from > DATE '2024-04-18'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        val,
        val_cume_dist,
        CASE WHEN val_cume_dist_plus_val >= 65 THEN 'High' WHEN val_cume_dist_plus_val >= 31 THEN 'Medium' ELSE 'Low' END AS val_cume_dist_plus_val_band,
        GREATEST(val, val_cume_dist) AS greatest_val_val_cume_dist
    FROM filtered
    WHERE effective_from >= TO_DATE('2024-03-01')
)
SELECT
    id,
    effective_from,
    val,
    val_cume_dist,
    greatest_val_val_cume_dist
FROM base;
