WITH attributed AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    INNER JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
with_windows AS (
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        SUM(promo_id) OVER (PARTITION BY DATE_TRUNC('week', ends_on), channel ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_promo_id
    FROM attributed
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_windows s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 7)
),
filtered AS (
    SELECT
        *,
        LOWER(promotion_promo_code_2) AS promotion_promo_code_2_lower,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM matched
    WHERE ends_on >= DATE '2023-07-06'
      AND promotion_promo_code IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), ends_on) / 30 AS months_since_ends_on,
        CONCAT(channel, ' ', promotion_promo_code_2) AS channel_promotion_promo_code_2,
        COALESCE(promotion_promo_code_2, 'NONE') AS promotion_promo_code_2_filled
    FROM filtered
    WHERE ends_on > DATE '2023-11-07'
      AND promotion_promo_code_2 IN ('WELCOME5', 'VIP')
)
SELECT
    DATE_TRUNC('MONTH', starts_on) AS activity_month,
    SUM(CASE WHEN promo_code = 'WELCOME5' THEN promotion_discount_pct ELSE 0 END) AS welcome5_promotion_discount_pct,
    COUNT(*) AS row_count,
    AVG(promo_id) FILTER (WHERE promotion_promo_code_2 = 'BUNDLE') AS bundle_avg_promo_id,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    SIZE(COLLECT_SET(promotion_promo_code_2)) AS num_promotion_promo_code_2s
FROM cleaned
GROUP BY activity_month
ORDER BY activity_month NULLS LAST;
