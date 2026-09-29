WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        LOWER(channel) AS channel_lower,
        CASE WHEN promo_code IN ('WELCOME5', 'VIP') THEN 'Tier 1' ELSE 'Other' END AS promo_code_group,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE ends_on >= DATE '2023-06-30'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_discount_pct,
        MIN(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_discount_pct
    FROM valid
),
enriched_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        ends_on,
        channel_lower,
        promo_code_group,
        resolved_channel,
        last4_avg_discount_pct,
        last4_count_discount_pct,
        last4_min_discount_pct,
        FIRST_VALUE(last4_min_discount_pct, TRUE) OVER w_running AS first_known_last4_min_discount_pct,
        FIRST_VALUE(last4_sum_discount_pct, TRUE) OVER w_hist AS first_known_last4_sum_discount_pct
    FROM enriched
    WINDOW w_running AS (PARTITION BY YEAR(ends_on), promo_code ORDER BY last4_sum_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY promo_code ORDER BY last4_sum_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(first_known_last4_sum_discount_pct) OVER (ORDER BY ends_on, id) AS prev_first_known_last4_sum_discount_pct,
        LAG(first_known_last4_min_discount_pct) OVER (ORDER BY ends_on, id) AS prev_first_known_last4_min_discount_pct,
        ROW_NUMBER() OVER (ORDER BY ends_on, id) AS seq_no
    FROM enriched_promotions
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN first_known_last4_sum_discount_pct >= 1.68 THEN 'LARGE' ELSE 'SMALL' END AS first_known_last4_sum_discount_pct_tier,
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count,
    ROUND(AVG(first_known_last4_sum_discount_pct), 2) AS avg_first_known_last4_sum_discount_pct,
    MIN(first_known_last4_sum_discount_pct) AS min_first_known_last4_sum_discount_pct,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles
FROM with_windows
GROUP BY CASE WHEN first_known_last4_sum_discount_pct >= 1.68 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', ends_on)
ORDER BY row_count
