WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id ASC) AS row_num
        FROM promotions
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(discount_pct) OVER w_recent last4_max_discount_pct,
        COUNT(discount_pct) OVER w_recent last4_count_discount_pct,
        SUM(discount_pct) OVER w_recent last4_sum_discount_pct,
        AVG(discount_pct) OVER w_recent last4_avg_discount_pct
    FROM ranked
    WINDOW w_recent AS (PARTITION BY channel ORDER BY discount_pct ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM enriched src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        row_num,
        last4_max_discount_pct,
        last4_sum_discount_pct,
        last4_avg_discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        CUME_DIST() OVER w_running promotion_discount_pct_cume_dist,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY promotion_promo_code), 4) promo_id_share,
        NTILE(10) OVER (PARTITION BY promotion_promo_code ORDER BY promotion_discount_pct ASC, id ASC, promo_id ASC) promotion_discount_pct_bucket
    FROM with_window
    WINDOW w_running AS (PARTITION BY channel ORDER BY promotion_discount_pct DESC NULLS FIRST)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        promo_code,
        ends_on,
        row_num,
        last4_max_discount_pct,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct_bucket,
        last4_sum_discount_pct - promo_id last4_sum_discount_pct_minus_promo_id
    FROM enriched_promotions
    WHERE promotion_promo_code IS NOT NULL
      AND promo_code IN ('SPRING10', 'VIP')
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('MONTH', ends_on) AS activity_month,
        DATE_TRUNC('YEAR', ends_on) AS year_start,
        SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes,
        APPROX_PERCENTILE(last4_sum_discount_pct_minus_promo_id, 0.9) AS last4_sum_discount_pct_minus_promo_id_p90,
        SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes_2,
        PERCENTILE_APPROX(last4_max_discount_pct, 0.9) AS last4_max_discount_pct_p90
    FROM cleaned
    GROUP BY 1, 2
    HAVING last4_max_discount_pct_p90 > 0
)
SELECT
    activity_month,
    year_start,
    num_promotion_promo_codes,
    last4_sum_discount_pct_minus_promo_id_p90,
    num_promotion_promo_codes_2,
    last4_max_discount_pct_p90
FROM grouped;
