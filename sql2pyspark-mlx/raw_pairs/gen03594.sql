WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(promo_code)) AS promo_code_title,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE ends_on >= TO_DATE('2023-10-13')
      AND channel IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount_pct,
        MIN(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_discount_pct,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_percent_rank
    FROM base
    WHERE discount_pct > 0.13
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        running_avg_discount_pct,
        discount_pct_percent_rank,
        NTILE(5) OVER (PARTITION BY channel ORDER BY running_avg_discount_pct, id) AS running_avg_discount_pct_bucket,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY running_avg_discount_pct, id) AS seq_no,
        FIRST_VALUE(running_min_discount_pct) IGNORE NULLS OVER (PARTITION BY channel, promo_code ORDER BY running_sum_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_min_discount_pct
    FROM metrics
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM scored e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL '30' DAY
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount_pct_percent_rank >= 0.76 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_percent_rank_tier,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    PERCENTILE_APPROX(discount_pct_percent_rank, 0.95) AS discount_pct_percent_rank_p95,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    ROUND(AVG(discount_pct_percent_rank), 2) AS avg_discount_pct_percent_rank
FROM attributed
GROUP BY discount_pct_percent_rank_tier
