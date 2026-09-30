WITH metrics AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        MAX(discount_pct) OVER w_part AS running_max_discount_pct,
        AVG(discount_pct) OVER w_part AS running_avg_discount_pct,
        MIN(discount_pct) OVER w_part AS running_min_discount_pct
    FROM promotions
    WHERE discount_pct < 0.16
    WINDOW w_part AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_promotions AS (
    SELECT
        *,
        COUNT(running_max_discount_pct) OVER w_grp AS last4_count_running_max_discount_pct,
        AVG(running_max_discount_pct) OVER w_grp AS last4_avg_running_max_discount_pct,
        MAX(running_max_discount_pct) OVER w_grp AS last4_max_running_max_discount_pct,
        MIN(running_max_discount_pct) OVER w_grp AS last4_min_running_max_discount_pct,
        AVG(running_avg_discount_pct) OVER w_part AS running_avg_discount_pct_avg_2_prior
    FROM metrics
    WINDOW w_grp AS (PARTITION BY channel ORDER BY running_max_discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY channel ORDER BY running_avg_discount_pct, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
valid AS (
    SELECT *, LAST_DAY(starts_on) AS starts_on_month_end
    FROM metrics_promotions
    WHERE channel IS NOT NULL
),
enriched AS (
    SELECT
        id,
        starts_on,
        running_min_discount_pct,
        last4_avg_running_max_discount_pct,
        last4_max_running_max_discount_pct,
        running_avg_discount_pct_avg_2_prior,
        AVG(last4_avg_running_max_discount_pct) OVER (ORDER BY running_avg_discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last4_avg_running_max_discount_pct_avg_2,
        LAST_VALUE(last4_avg_running_max_discount_pct) IGNORE NULLS OVER (ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last4_avg_running_max_discount_pct
    FROM valid
)
SELECT
    id,
    starts_on,
    last4_max_running_max_discount_pct,
    running_avg_discount_pct_avg_2_prior,
    last4_avg_running_max_discount_pct_avg_2,
    last_known_last4_avg_running_max_discount_pct
FROM enriched
ORDER BY running_avg_discount_pct_avg_2_prior NULLS LAST
