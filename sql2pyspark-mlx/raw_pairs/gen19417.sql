WITH matched AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.ends_on >= pr.starts_on
       AND s.ends_on <= DATE_ADD(pr.ends_on, 10)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY starts_on DESC, id, promo_id) AS row_num
    FROM matched
    QUALIFY row_num = 1
),
metrics AS (
    SELECT
        id,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        row_num,
        FIRST_VALUE(promotion_discount_pct, TRUE) OVER (ORDER BY ends_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_promotion_discount_pct,
        SUM(discount_pct) OVER (ORDER BY row_num, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        COUNT(promo_id) OVER (PARTITION BY channel) AS channel_count_promo_id,
        LEAD(promotion_discount_pct, 1, 0) OVER (PARTITION BY channel ORDER BY discount_pct, id, promo_id) AS next_promotion_discount_pct,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct DESC) AS promotion_discount_pct_cume_dist
    FROM ranked
),
metrics_promotions AS (
    SELECT
        id,
        ends_on,
        promo_id,
        promotion_promo_code,
        first_known_promotion_discount_pct,
        next_promotion_discount_pct,
        MAX(first_known_promotion_discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY running_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_promotion_discount_pct,
        SUM(first_known_promotion_discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY running_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_first_known_promotion_discount_pct,
        MIN(first_known_promotion_discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY running_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_first_known_promotion_discount_pct,
        AVG(first_known_promotion_discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY running_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_first_known_promotion_discount_pct
    FROM metrics
)
SELECT
    promo_id,
    COUNT_IF(promotion_promo_code = 'WELCOME5') AS welcome5_events,
    COUNT(DISTINCT promotion_promo_code) AS distinct_promotion_promo_codes,
    COUNT(*) FILTER (WHERE promotion_promo_code = 'SPRING10') AS spring10_rows
FROM metrics_promotions
GROUP BY promo_id;
