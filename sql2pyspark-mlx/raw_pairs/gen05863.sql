WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL '30' DAY
),
attributed_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'store') = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL 30 DAYS
),
attributed_promotions_2 AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM attributed_promotions a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_4,
        pr.promo_code AS promotion_promo_code_4,
        pr.discount_pct AS promotion_discount_pct_4
    FROM attributed_promotions_2 s
    INNER JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '30' DAY
)
-- Multi-level aggregation
SELECT
    CASE WHEN promotion_discount_pct >= 0.27 THEN 'PREMIUM' ELSE 'STANDARD' END AS promotion_discount_pct_tier,
    promotion_promo_code_4,
    promo_id,
    SUM(CASE WHEN promotion_promo_code_3 = 'WELCOME5' THEN promotion_discount_pct_3 ELSE 0 END) AS welcome5_promotion_discount_pct_3,
    COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count,
    COUNT(DISTINCT promotion_promo_code_4) AS distinct_promotion_promo_code_4s,
    SORT_ARRAY(COLLECT_SET(promotion_promo_code_3)) AS promotion_promo_code_3_list,
    COUNT(*) AS event_count
FROM matched
GROUP BY 1, 2, 3
HAVING SUM(CASE WHEN promotion_promo_code_3 = 'WELCOME5' THEN promotion_discount_pct_3 ELSE 0 END) >= 0
