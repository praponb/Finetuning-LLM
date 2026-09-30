WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL '7' DAY
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(promotion_discount_pct) OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_promotion_discount_pct,
        MAX(promotion_discount_pct) OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_promotion_discount_pct,
        AVG(promotion_discount_pct) OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_promotion_discount_pct
    FROM matched
    WHERE channel = 'store'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(last4_max_promotion_discount_pct) OVER w_hist AS last4_max_promotion_discount_pct_avg_5_prior,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
        ROUND(last4_max_promotion_discount_pct / SUM(last4_max_promotion_discount_pct) OVER (PARTITION BY promo_code), 4) AS last4_max_promotion_discount_pct_share,
        FIRST_VALUE(promotion_discount_pct) IGNORE NULLS OVER w_grp AS first_known_promotion_discount_pct,
        AVG(promotion_discount_pct) OVER w_part AS promotion_discount_pct_avg_6_prior
    FROM metrics
    WINDOW w_hist AS (PARTITION BY promo_code ORDER BY ends_on, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY promo_code, promotion_promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY promo_code ORDER BY discount_pct, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        last4_min_promotion_discount_pct,
        last4_max_promotion_discount_pct_avg_5_prior,
        discount_pct_share,
        last4_max_promotion_discount_pct_share,
        first_known_promotion_discount_pct,
        promotion_discount_pct_avg_6_prior,
        DATE_ADD(starts_on, 79) AS starts_on_plus_79d,
        ROUND(last4_max_promotion_discount_pct_share / last4_avg_promotion_discount_pct, 2) AS last4_max_promotion_discount_pct_share_per_last4_avg_promotion_discount_pct
    FROM with_windows
    WHERE ends_on > DATE '2023-07-27'
)
-- Grouped metrics
SELECT
    CASE WHEN discount_pct >= 0.34 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    PERCENTILE_APPROX(last4_min_promotion_discount_pct, 0.5) AS last4_min_promotion_discount_pct_p50,
    MAX(last4_min_promotion_discount_pct) AS max_last4_min_promotion_discount_pct,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    AVG(last4_min_promotion_discount_pct) FILTER (WHERE channel = 'mobile') AS mobile_avg_last4_min_promotion_discount_pct
FROM cleaned
GROUP BY discount_pct_tier
ORDER BY partner_count DESC NULLS FIRST
