WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount_pct) AS discount_pct_dense_rank,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        AVG(discount_pct) OVER (PARTITION BY YEAR(ends_on), channel ORDER BY ends_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_pct_avg_3
    FROM promotions
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM metrics e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on <= DATE_ADD(pr.ends_on, 7)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_window
    EXCEPT DISTINCT
    SELECT id
    FROM promotions
    WHERE MONTH(ends_on) IN (3, 4, 8)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_window s
    JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(promotion_promo_code, ' - ', promo_code) AS promotion_promo_code_promo_code,
        FLOOR(promotion_discount_pct) AS promotion_discount_pct_whole,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM verified
    WHERE ends_on > DATE '2023-08-27'
      AND promotion_promo_code IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo_code, promotion_promo_code ORDER BY promotion_discount_pct_whole DESC NULLS LAST) AS promotion_discount_pct_whole_percent_rank,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct_whole DESC) AS promotion_discount_pct_whole_dense_rank
    FROM base
)
SELECT
    channel,
    starts_on,
    ends_on,
    discount_pct,
    discount_pct_dense_rank,
    first_known_channel,
    discount_pct_avg_3,
    promo_id,
    promotion_promo_code,
    CASE WHEN channel RLIKE '^(web|partner)$' THEN 'EXTENDED' WHEN channel RLIKE '^(mobile|store)$' THEN 'CORE' ELSE 'GROUP_A' END AS channel_zone
FROM scored
ORDER BY discount_pct NULLS FIRST, channel DESC
