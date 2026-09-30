WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL '30' DAY
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, channel
    FROM attributed
    INTERSECT ALL
    SELECT id, channel
    FROM promotions
    WHERE ends_on < '2025-02-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM attributed s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.channel = k.channel
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY channel), 4) AS promo_id_share,
        AVG(discount_pct) OVER (PARTITION BY channel) AS channel_avg_discount_pct,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY promo_id) AS promo_id_percent_rank
    FROM kept_rows
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        channel,
        promo_code,
        discount_pct,
        promo_id,
        promotion_promo_code,
        promo_id_share,
        channel_avg_discount_pct,
        promo_id_percent_rank,
        CUME_DIST() OVER (PARTITION BY promotion_promo_code, channel ORDER BY promo_id_share DESC) AS promo_id_share_cume_dist,
        MAX(channel_avg_discount_pct) OVER (PARTITION BY DATE_TRUNC('WEEK', starts_on), channel) AS starts_on_period_max_channel_avg_discount_pct,
        ROUND(promotion_discount_pct / SUM(promotion_discount_pct) OVER (PARTITION BY MONTH(starts_on), channel), 4) AS promotion_discount_pct_share,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct DESC NULLS FIRST) AS promotion_discount_pct_percent_rank
    FROM scored
)
-- Grouped metrics
SELECT
    promo_id,
    CASE WHEN starts_on_period_max_channel_avg_discount_pct >= 0 THEN 'LARGE' ELSE 'SMALL' END AS starts_on_period_max_channel_avg_discount_pct_tier,
    APPROX_PERCENTILE(starts_on_period_max_channel_avg_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS starts_on_period_max_channel_avg_discount_pct_quartiles,
    SUM(CASE WHEN promotion_promo_code = 'VIP' THEN promotion_discount_pct_share ELSE 0 END) AS vip_promotion_discount_pct_share,
    MAX(starts_on_period_max_channel_avg_discount_pct) AS max_starts_on_period_max_channel_avg_discount_pct
FROM with_windows
GROUP BY promo_id, starts_on_period_max_channel_avg_discount_pct_tier
