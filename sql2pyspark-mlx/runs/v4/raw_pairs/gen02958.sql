WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(discount_pct, 2) AS discount_pct_rounded,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE channel NOT IN ('partner', 'store')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_rounded DESC, id) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM latest s
    LEFT JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '14' DAY
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        SUM(promo_id) OVER (PARTITION BY promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_promo_id,
        ROUND(promotion_discount_pct / SUM(promotion_discount_pct) OVER (PARTITION BY channel), 4) AS promotion_discount_pct_share,
        NTILE(5) OVER w_running AS promotion_discount_pct_bucket
    FROM matched
    WINDOW w_ordered AS (ORDER BY ends_on, id, promo_id), w_running AS (PARTITION BY promotion_promo_code, promo_code ORDER BY promotion_discount_pct, id, promo_id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code, channel ORDER BY discount_pct_rounded DESC, id, promo_id) AS row_seq,
        FIRST_VALUE(cumulative_promo_id, TRUE) OVER (PARTITION BY channel, promotion_promo_code ORDER BY discount_pct_rounded, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_promo_id,
        AVG(seq_no) OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct_share, id, promo_id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS seq_no_avg_5
    FROM enriched
)
SELECT
    id,
    channel,
    ends_on,
    discount_pct,
    discount_pct_rounded,
    resolved_channel,
    row_num,
    promo_id,
    promotion_promo_code,
    CASE WHEN channel RLIKE '^(mobile|web)$' THEN 'EXTENDED' WHEN channel RLIKE '^(store|partner)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS channel_zone
FROM metrics;
