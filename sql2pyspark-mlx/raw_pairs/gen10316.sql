WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', promo_code, channel) AS promo_code_channel_key,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM promotions
    WHERE promo_code = 'VIP'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(channel) IGNORE NULLS OVER (ORDER BY ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct ASC) AS discount_pct_cume_dist
    FROM cleaned
    WHERE promo_code = 'VIP'
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM with_windows a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'store') = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on < pr.ends_on + INTERVAL 30 DAYS
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct DESC, id ASC, promo_id ASC) AS rank_in_group
    FROM with_window
    QUALIFY rank_in_group = 1
),
with_window_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM deduped e
    INNER JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.starts_on BETWEEN pr.starts_on AND pr.ends_on
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promotion_promo_code ORDER BY starts_on DESC, id ASC, promo_id ASC, promo_id_2 ASC) AS rank_in_group_2
    FROM with_window_promotions
    QUALIFY rank_in_group_2 = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, channel
    FROM latest
    INTERSECT
    SELECT id, channel
    FROM promotions
    WHERE promo_code NOT IN ('VIP', 'SPRING10')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.id = k.id AND s.channel = k.channel
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN promo_id_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_id_2_tier,
    DATE_TRUNC('WEEK', ends_on) AS week_start,
    SIZE(COLLECT_SET(promotion_promo_code_2)) AS num_promotion_promo_code_2s,
    APPROX_PERCENTILE(discount_pct, 0.5) AS discount_pct_p50
FROM kept_rows
GROUP BY CASE WHEN promo_id_2 >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', ends_on)
ORDER BY num_promotion_promo_code_2s DESC NULLS LAST;
