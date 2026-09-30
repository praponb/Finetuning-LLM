WITH matched AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    INNER JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
with_windows AS (
    SELECT
        *,
        MIN(discount_pct) OVER w_part AS running_min_discount_pct,
        AVG(discount_pct) OVER w_part AS running_avg_discount_pct,
        MAX(discount_pct) OVER w_part AS running_max_discount_pct
    FROM matched
    WHERE YEAR(starts_on) = 2024
    WINDOW w_part AS (PARTITION BY promotion_promo_code, promo_code ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        starts_on,
        promo_id,
        running_avg_discount_pct,
        running_max_discount_pct,
        LAG(discount_pct) OVER w_grp AS prev_discount_pct,
        LEAD(running_min_discount_pct) OVER w_grp AS next_running_min_discount_pct,
        ROW_NUMBER() OVER w_grp AS row_seq,
        FIRST_VALUE(running_min_discount_pct, TRUE) OVER w_running AS first_known_running_min_discount_pct
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY starts_on, id, promo_id), w_running AS (ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    SELECT
        id,
        starts_on,
        promo_id,
        running_avg_discount_pct,
        running_max_discount_pct,
        row_seq,
        first_known_running_min_discount_pct,
        ROUND(running_max_discount_pct * 0.85, 2) AS running_max_discount_pct_scaled
    FROM enriched
    WHERE starts_on >= DATE '2023-08-26'
),
cleaned_promotions AS (
    SELECT
        id,
        starts_on,
        promo_id,
        running_max_discount_pct,
        running_max_discount_pct_scaled,
        COALESCE(first_known_running_min_discount_pct, 0) AS first_known_running_min_discount_pct_filled,
        running_max_discount_pct_scaled - running_avg_discount_pct AS running_max_discount_pct_scaled_minus_running_avg_discount_pct
    FROM cleaned
    WHERE running_max_discount_pct_scaled BETWEEN 23 AND 47
)
SELECT
    CASE WHEN promo_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS promo_id_tier,
    CASE WHEN running_max_discount_pct_scaled >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_discount_pct_scaled_tier,
    COUNT(*) AS cnt,
    AVG(first_known_running_min_discount_pct_filled) AS avg_first_known_running_min_discount_pct_filled,
    PERCENTILE_APPROX(promo_id, ARRAY(0.25, 0.5, 0.75)) AS promo_id_quartiles,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN promo_id >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(*) DESC) AS cnt_percent_rank
FROM cleaned_promotions
GROUP BY CASE WHEN promo_id >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN running_max_discount_pct_scaled >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY running_max_discount_pct_scaled_tier DESC NULLS LAST;
