WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', promo_code, channel) AS promo_code_channel_key,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE discount_pct BETWEEN 0.14 AND 0.15
      AND ends_on > DATE '2023-12-08'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM valid s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        promo_code_channel_key,
        resolved_channel,
        promo_id,
        promotion_promo_code,
        COUNT(promo_id) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_promo_id,
        SUM(promo_id) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_promo_id,
        MAX(promo_id) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_promo_id,
        AVG(promo_id) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_promo_id
    FROM with_window
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY promotion_promo_code ORDER BY running_avg_promo_id DESC NULLS LAST) AS rn
        FROM metrics
    ) x
    WHERE rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(running_avg_promo_id) OVER (PARTITION BY promo_code ORDER BY running_count_promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_avg_promo_id,
        AVG(running_avg_promo_id) OVER (PARTITION BY promo_code ORDER BY running_count_promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_avg_promo_id,
        SUM(running_avg_promo_id) OVER (PARTITION BY promo_code ORDER BY running_count_promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_avg_promo_id,
        COUNT(running_avg_promo_id) OVER (PARTITION BY promo_code ORDER BY running_count_promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_avg_promo_id,
        AVG(running_count_promo_id) OVER (PARTITION BY promo_code ORDER BY ends_on, id, promo_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_count_promo_id_avg_3_prior
    FROM ranked
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_avg_promo_id >= 10.4 THEN 'HIGH' ELSE 'LOW' END AS running_avg_promo_id_tier,
        promo_id,
        CASE WHEN discount_pct >= 0.27 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_tier,
        COUNT(CASE WHEN promotion_promo_code = 'VIP' THEN 1 END) AS vip_count,
        COUNT_IF(promotion_promo_code = 'BUNDLE') AS bundle_events,
        SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
        SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes
    FROM scored
    GROUP BY running_avg_promo_id_tier, promo_id, discount_pct_tier
    HAVING SIZE(COLLECT_SET(promotion_promo_code)) >= 1
)
SELECT
    running_avg_promo_id_tier,
    promo_id,
    discount_pct_tier,
    bundle_events,
    promo_code_list,
    num_promotion_promo_codes
FROM grouped
ORDER BY discount_pct_tier;
