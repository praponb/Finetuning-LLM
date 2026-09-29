WITH matched_keys AS (
    -- Set intersection on keys
    SELECT promo_code, channel
    FROM promotions
    INTERSECT ALL
    SELECT promo_code, channel
    FROM promotions
    WHERE ends_on >= '2024-09-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN matched_keys k ON s.promo_code = k.promo_code AND s.channel = k.channel
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM reconciled e
    INNER JOIN promotions pr
        ON COALESCE(e.channel, 'store') = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
with_window_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
metrics AS (
    -- Ranking and gap features
    SELECT *, ROUND(promo_id_2 / SUM(promo_id_2) OVER (PARTITION BY promotion_promo_code_2), 4) AS promo_id_2_share
    FROM with_window_promotions
    WHERE discount_pct > 0.33
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM metrics s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '7' DAY
)
-- Aggregate with conditional metrics
SELECT
    channel,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    SUM(CASE WHEN promotion_promo_code_3 = 'WELCOME5' THEN promo_id_3 ELSE 0 END) AS welcome5_promo_id_3,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct_2,
    AVG(discount_pct) FILTER (WHERE promotion_promo_code = 'BUNDLE') AS bundle_avg_discount_pct,
    AVG(promo_id_2) FILTER (WHERE channel = 'web') AS web_avg_promo_id_2,
    PERCENTILE_APPROX(promotion_discount_pct_2, 0.9) AS promotion_discount_pct_2_p90
FROM attributed
GROUP BY channel
HAVING ROUND(AVG(discount_pct), 2) < 1000000
ORDER BY avg_discount_pct DESC NULLS FIRST;
