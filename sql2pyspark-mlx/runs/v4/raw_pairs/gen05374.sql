WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS dedup_rank
        FROM promotions
    ) t
    WHERE dedup_rank = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM deduped src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.ends_on >= pr.starts_on
       AND src.ends_on < pr.ends_on + INTERVAL '14' DAY
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY promotion_promo_code, promo_code), 4) AS promo_id_share,
        AVG(promo_id) OVER (ORDER BY promo_id, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS promo_id_avg_6_prior
    FROM with_window
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(promo_id * 1.1, 2) AS promo_id_adjusted,
        COALESCE(channel, 'unassigned') AS channel_filled,
        COALESCE(promotion_promo_code, 'OTHER') AS promotion_promo_code_clean
    FROM scored
    WHERE ends_on > DATE '2023-12-05'
      AND promo_code NOT IN ('BUNDLE', 'WELCOME5')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        dedup_rank,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_share,
        promo_id_avg_6_prior,
        promo_id_adjusted,
        channel_filled,
        promotion_promo_code_clean,
        AVG(promo_id_avg_6_prior) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS promo_id_avg_6_prior_avg_5_prior,
        CUME_DIST() OVER w_recent AS promo_id_avg_6_prior_cume_dist,
        NTILE(5) OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY discount_pct DESC, id, promo_id) AS discount_pct_bucket
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY promotion_promo_code, promo_code ORDER BY promo_id_avg_6_prior)
),
with_window_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_windows src
    JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
)
-- Grouped metrics
SELECT
    promo_id,
    DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
    APPROX_PERCENTILE(promo_id_adjusted, 0.95) AS promo_id_adjusted_p95,
    COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END) AS welcome5_count,
    ROUND(AVG(promo_id_avg_6_prior_avg_5_prior), 2) AS avg_promo_id_avg_6_prior_avg_5_prior,
    SORT_ARRAY(COLLECT_SET(promotion_promo_code)) AS promotion_promo_code_list
FROM with_window_promotions
GROUP BY GROUPING SETS ((promo_id, DATE_TRUNC('QUARTER', starts_on)), (promo_id), ())
ORDER BY quarter_start;
