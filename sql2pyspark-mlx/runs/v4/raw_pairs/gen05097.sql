WITH cleaned AS (
    SELECT
        *,
        DATE_TRUNC('MONTH', starts_on) AS starts_on_period,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE starts_on > DATE '2023-10-05'
      AND promo_code IN ('BUNDLE', 'WELCOME5', 'VIP')
)
SELECT
    channel,
    CASE WHEN discount_pct >= 0.3 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    promo_code,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    MIN(starts_on_period) AS first_seen,
    ROUND(AVG(discount_pct), 2) AS avg_discount_pct,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    COUNT_IF(channel = 'web') AS web_events
FROM cleaned
GROUP BY channel, CASE WHEN discount_pct >= 0.3 THEN 'HIGH' ELSE 'LOW' END, promo_code;
