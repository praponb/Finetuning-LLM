WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
metrics AS (
    SELECT
        plan,
        effective_from,
        currency,
        val,
        SUM(val) OVER (PARTITION BY DATE_TRUNC('week', effective_from)) AS effective_from_period_sum_val,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val NULLS LAST) AS val_cume_dist,
        COUNT(val) OVER (PARTITION BY currency) AS currency_count_val,
        RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', effective_from), currency ORDER BY val NULLS FIRST) AS val_rank
    FROM exploded
),
cleaned AS (
    SELECT
        effective_from,
        val,
        effective_from_period_sum_val,
        plan_count_val,
        val_rank,
        INITCAP(TRIM(currency)) AS currency_title
    FROM metrics
    WHERE effective_from > DATE '2024-04-20'
),
base AS (
    SELECT
        effective_from,
        effective_from_period_sum_val,
        val_rank,
        currency_title,
        DATE_ADD(effective_from, 57) AS effective_from_plus_57d,
        val + effective_from_period_sum_val AS val_plus_effective_from_period_sum_val
    FROM cleaned
    WHERE effective_from >= TO_DATE('2024-05-28')
      AND val >= 49
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY val_plus_effective_from_period_sum_val NULLS LAST) AS val_plus_effective_from_period_sum_val_cume_dist,
        CUME_DIST() OVER (ORDER BY val_rank DESC NULLS FIRST) AS val_rank_cume_dist
    FROM base
),
with_windows_price_tiers AS (
    SELECT
        *,
        CUME_DIST() OVER w_running AS val_plus_effective_from_period_sum_val_cume_dist_cume_dist,
        DENSE_RANK() OVER (ORDER BY val_plus_effective_from_period_sum_val NULLS FIRST) AS val_plus_effective_from_period_sum_val_dense_rank
    FROM with_windows
    WINDOW w_running AS (ORDER BY val_plus_effective_from_period_sum_val_cume_dist NULLS FIRST)
),
base_price_tiers AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) / 365.25 AS years_since_effective_from
    FROM with_windows_price_tiers
    WHERE val_rank = 4
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    COUNT(*) AS event_count,
    AVG(val_rank) AS avg_val_rank,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(val_plus_effective_from_period_sum_val, ARRAY(0.25, 0.5, 0.75)) AS val_plus_effective_from_period_sum_val_quartiles,
    MAX(val_rank_cume_dist) AS max_val_rank_cume_dist,
    PERCENTILE_APPROX(val_rank_cume_dist, 0.9) AS val_rank_cume_dist_p90
FROM base_price_tiers
GROUP BY quarter_start
ORDER BY val_rank_cume_dist_p90 DESC;
