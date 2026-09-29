WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions src
    INNER JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL '7' DAY
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(discount_pct, 2) OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct, id, promo_id) AS next_discount_pct,
        SUM(promotion_discount_pct) OVER (PARTITION BY YEAR(starts_on) ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_promotion_discount_pct,
        LEAD(promotion_discount_pct, 1) OVER (PARTITION BY MONTH(ends_on), channel ORDER BY ends_on, id, promo_id) AS next_promotion_discount_pct
    FROM with_window
    WHERE promo_code = 'BUNDLE'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY next_promotion_discount_pct DESC NULLS LAST, id, promo_id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(promo_id, 2, 0) OVER w_running AS prev_promo_id,
        LAST_VALUE(next_discount_pct) IGNORE NULLS OVER (PARTITION BY promotion_promo_code ORDER BY ends_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_discount_pct,
        LEAD(discount_pct, 1) OVER (ORDER BY ends_on, id, promo_id) AS next_discount_pct_2
    FROM latest
    WINDOW w_running AS (PARTITION BY channel ORDER BY ends_on, id, promo_id)
),
latest_promotions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        cumulative_promotion_discount_pct,
        prev_promo_id,
        last_known_next_discount_pct
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY row_num DESC, id, promo_id) = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct
    FROM latest_promotions src
    INNER JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        starts_on,
        promo_id,
        promotion_discount_pct,
        cumulative_promotion_discount_pct,
        prev_promo_id,
        last_known_next_discount_pct,
        promo_id_2,
        CUME_DIST() OVER (ORDER BY discount_pct NULLS FIRST) AS discount_pct_cume_dist,
        LEAD(cumulative_promotion_discount_pct, 2) OVER (PARTITION BY YEAR(starts_on), promotion_promo_code_2 ORDER BY cumulative_promotion_discount_pct, id, promo_id, promo_id_2) AS next_cumulative_promotion_discount_pct,
        AVG(promo_id_2) OVER (PARTITION BY channel ORDER BY promo_id_2, id, promo_id, promo_id_2 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS promo_id_2_avg_3
    FROM matched
)
-- Multi-level aggregation
SELECT
    CASE WHEN promo_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_id_tier,
    COUNT(*) AS event_count,
    COUNT(*) AS event_count_2,
    ROUND(AVG(promo_id_2_avg_3), 2) AS avg_promo_id_2_avg_3,
    APPROX_PERCENTILE(promotion_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS promotion_discount_pct_quartiles,
    ROUND(AVG(prev_promo_id), 2) AS avg_prev_promo_id,
    COUNT(*) AS cnt
FROM enriched_promotions
GROUP BY CASE WHEN promo_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(*) < 1000
ORDER BY avg_prev_promo_id;
