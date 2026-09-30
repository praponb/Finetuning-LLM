WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 7 DAYS
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        promo_id,
        promotion_promo_code,
        MIN(promotion_discount_pct) OVER w_running running_min_promotion_discount_pct,
        SUM(promotion_discount_pct) OVER w_running running_sum_promotion_discount_pct,
        MAX(promotion_discount_pct) OVER w_running running_max_promotion_discount_pct,
        AVG(promotion_discount_pct) OVER w_running running_avg_promotion_discount_pct
    FROM attributed
    WHERE promo_id = 8
    WINDOW w_running AS (PARTITION BY promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct
    FROM enriched e
    LEFT JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        promo_id,
        running_sum_promotion_discount_pct,
        running_avg_promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        discount_pct,
        CASE WHEN running_avg_promotion_discount_pct >= 0.34 THEN 'High' WHEN running_avg_promotion_discount_pct >= 0.18 THEN 'Medium' ELSE 'Low' END running_avg_promotion_discount_pct_level
    FROM matched
    WHERE promotion_promo_code_2 IS DISTINCT FROM 'SPRING10'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promotion_promo_code_2 ORDER BY running_avg_promotion_discount_pct NULLS FIRST, id, promo_id, promo_id_2) = 1
),
deduped_promotions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        promo_id,
        running_sum_promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code_2 ORDER BY promo_id DESC NULLS FIRST, id, promo_id, promo_id_2) AS row_num
        FROM deduped
    ) t
    WHERE row_num = 1
)
-- Running and moving aggregates
SELECT
    id,
    promo_id,
    promo_id_2,
    promotion_promo_code_2,
    LAG(discount_pct) OVER w_recent prev_discount_pct,
    LEAD(promo_id) OVER w_recent next_promo_id,
    LAG(promo_id) OVER w_recent prev_promo_id,
    ROW_NUMBER() OVER w_recent event_seq
FROM deduped_promotions
WINDOW w_recent AS (PARTITION BY promotion_promo_code_2 ORDER BY discount_pct, id, promo_id, promo_id_2)
