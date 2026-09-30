WITH valid AS (
    SELECT *, ROUND(discount_pct, 2) discount_pct_rounded
    FROM promotions
    WHERE ends_on > DATE '2023-05-30'
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid a
    JOIN promotions pr
        ON a.channel = pr.channel
       AND a.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM attributed e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM with_window s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'store') = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 7)
),
metrics AS (
    SELECT
        id,
        starts_on,
        discount_pct,
        promo_id,
        promo_id_2,
        promo_id_3,
        promotion_promo_code_3,
        promotion_discount_pct_3,
        MAX(promotion_discount_pct) OVER w_grp last3_max_promotion_discount_pct,
        AVG(promotion_discount_pct) OVER w_grp last3_avg_promotion_discount_pct,
        COUNT(promotion_discount_pct) OVER w_grp last3_count_promotion_discount_pct,
        SUM(promotion_discount_pct) OVER w_grp last3_sum_promotion_discount_pct
    FROM matched
    WHERE promotion_promo_code_3 NOT IN ('BUNDLE', 'WELCOME5')
    WINDOW w_grp AS (PARTITION BY promotion_promo_code_3 ORDER BY promotion_discount_pct_3, id, promo_id, promo_id_2, promo_id_3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    discount_pct,
    promo_id,
    promo_id_2,
    promo_id_3,
    promotion_promo_code_3,
    promotion_discount_pct_3,
    last3_max_promotion_discount_pct,
    last3_avg_promotion_discount_pct,
    CASE WHEN promotion_promo_code_3 RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS promotion_promo_code_3_zone
FROM metrics
ORDER BY promotion_discount_pct_3 DESC NULLS FIRST, promo_id_3;
