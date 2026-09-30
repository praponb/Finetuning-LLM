WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window src
    INNER JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_id,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        CAST(promotion_discount_pct_2 AS INT) AS promotion_discount_pct_2_int,
        ROUND(promotion_discount_pct_2, 0) AS promotion_discount_pct_2_rounded
    FROM attributed
    WHERE MONTH(starts_on) IN (4, 5, 9)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        promo_id,
        promotion_discount_pct,
        promo_id_2,
        promotion_discount_pct_2_int,
        promotion_discount_pct_2_rounded,
        ROUND(promotion_discount_pct / promo_id_2, 2) AS promotion_discount_pct_per_promo_id_2
    FROM cleaned
    WHERE promotion_promo_code_2 IN ('VIP', 'BUNDLE')
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', starts_on) AS activity_month,
    CASE WHEN promotion_discount_pct_per_promo_id_2 >= 84 THEN 'LARGE' ELSE 'SMALL' END AS promotion_discount_pct_per_promo_id_2_tier,
    COUNT_IF(channel = 'store') AS store_events,
    COUNT_IF(channel = 'web') AS web_events,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(promotion_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS promotion_discount_pct_quartiles,
    GROUPING_ID() AS grouping_level
FROM valid
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', starts_on), CASE WHEN promotion_discount_pct_per_promo_id_2 >= 84 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('MONTH', starts_on)), ())
HAVING web_events < 1000
ORDER BY mobile_count NULLS LAST;
