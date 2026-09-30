WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        LOWER(promo_code) AS promo_code_lower,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM promotions
    WHERE ends_on <= '2025-10-07'
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(is_store, TRUE) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_store,
        LAG(discount_pct, 1) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS prev_discount_pct,
        LEAD(discount_pct, 2) OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id) AS next_discount_pct
    FROM base
),
valid AS (
    SELECT
        *,
        ROUND(is_store, 2) AS is_store_rounded,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM metrics
    WHERE channel IS NOT NULL
),
valid_promotions AS (
    SELECT *, is_store_rounded / NULLIF(first_known_is_store, 0) AS is_store_rounded_per_first_known_is_store
    FROM valid
    WHERE starts_on > TO_DATE('2023-05-07')
      AND is_store_rounded <= 39
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY is_store DESC NULLS LAST) AS is_store_dense_rank,
        LAST_VALUE(channel) IGNORE NULLS OVER (ORDER BY first_known_is_store, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        SUM(next_discount_pct) OVER (PARTITION BY promo_code ORDER BY is_store_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_discount_pct
    FROM valid_promotions
    WHERE prev_discount_pct >= 0.31
)
SELECT
    channel,
    AVG(discount_pct) FILTER (WHERE promo_code = 'VIP') AS vip_avg_discount_pct,
    SUM(CASE WHEN channel = 'web' THEN discount_pct ELSE 0 END) AS web_discount_pct,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    APPROX_PERCENTILE(is_store_rounded, 0.9) AS is_store_rounded_p90,
    PERCENTILE_APPROX(prev_discount_pct, 0.9) AS prev_discount_pct_p90,
    AVG(discount_pct) FILTER (WHERE promo_code = 'SPRING10') AS spring10_avg_discount_pct
FROM with_windows
GROUP BY channel
ORDER BY channel DESC
