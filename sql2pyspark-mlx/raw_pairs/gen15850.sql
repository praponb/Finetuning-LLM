WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(effective_from, 'MMM yyyy') AS effective_from_label,
        SUBSTRING(currency, 1, 3) AS currency_prefix
    FROM lines
    WHERE effective_from >= '2024-05-01'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        currency,
        val,
        effective_from_label,
        currency_prefix,
        COALESCE(val, 0) AS val_filled
    FROM cleaned
    WHERE effective_from >= DATE '2024-02-28'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY val_filled DESC NULLS FIRST) AS val_filled_cume_dist,
        MIN(val_filled) OVER (PARTITION BY currency) AS currency_min_val_filled,
        COUNT(val_filled) OVER (PARTITION BY currency) AS currency_count_val_filled
    FROM base
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT *, AVG(val) OVER (PARTITION BY currency) AS currency_avg_val
    FROM enriched
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, currency_count_val_filled / NULLIF(currency_avg_val, 0) AS currency_count_val_filled_per_currency_avg_val
    FROM enriched_price_tiers
    WHERE effective_from >= TO_DATE('2024-06-18')
      AND val_filled > 46
),
metrics AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        val,
        currency_prefix,
        val_filled,
        val_filled_cume_dist,
        currency_min_val_filled,
        SUM(val_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', effective_from)) AS effective_from_period_sum_val_filled,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val_2,
        RANK() OVER (PARTITION BY currency ORDER BY currency_count_val_filled_per_currency_avg_val DESC NULLS FIRST) AS currency_count_val_filled_per_currency_avg_val_rank,
        CUME_DIST() OVER (ORDER BY currency_min_val_filled NULLS LAST) AS currency_min_val_filled_cume_dist,
        RANK() OVER w_part AS val_filled_rank
    FROM filtered
    WINDOW w_part AS (PARTITION BY currency ORDER BY val_filled DESC)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    CASE WHEN val_filled_cume_dist >= 0.79 THEN 'HIGH' ELSE 'LOW' END AS val_filled_cume_dist_tier,
    APPROX_PERCENTILE(currency_min_val_filled_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS currency_min_val_filled_cume_dist_quartiles,
    ROUND(AVG(effective_from_period_sum_val_filled), 2) AS avg_effective_from_period_sum_val_filled,
    ROUND(AVG(currency_avg_val_2), 2) AS avg_currency_avg_val_2,
    APPROX_PERCENTILE(currency_min_val_filled, ARRAY(0.25, 0.5, 0.75)) AS currency_min_val_filled_quartiles,
    COUNT(*) AS event_count
FROM metrics
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', effective_from), CASE WHEN val_filled_cume_dist >= 0.79 THEN 'HIGH' ELSE 'LOW' END);
