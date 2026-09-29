WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(promo_code) AS promo_code_length,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE channel IS DISTINCT FROM 'web'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        promo_code_length,
        channel_filled,
        MAX(promo_code_length) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_promo_code_length,
        SUM(promo_code_length) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_promo_code_length,
        COUNT(promo_code_length) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_promo_code_length,
        AVG(promo_code_length) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_promo_code_length
    FROM filtered
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        promo_code_length,
        channel_filled,
        running_sum_promo_code_length,
        COALESCE(running_max_promo_code_length, 0) AS running_max_promo_code_length_filled
    FROM with_windows
    WHERE channel IS DISTINCT FROM 'store'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned s
    INNER JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on BETWEEN pr.starts_on AND pr.ends_on
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_sum_promo_code_length >= 370 THEN 'HIGH' ELSE 'LOW' END AS running_sum_promo_code_length_tier,
    COUNT_IF(promotion_promo_code = 'WELCOME5') AS welcome5_events,
    SUM(CASE WHEN promotion_promo_code = 'SPRING10' THEN promo_code_length ELSE 0 END) AS spring10_promo_code_length,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    APPROX_PERCENTILE(promo_code_length, ARRAY(0.25, 0.5, 0.75)) AS promo_code_length_quartiles,
    GROUPING_ID() AS gid
FROM matched
GROUP BY ROLLUP (CASE WHEN running_sum_promo_code_length >= 370 THEN 'HIGH' ELSE 'LOW' END);
