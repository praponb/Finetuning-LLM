WITH deduped AS (
    SELECT *
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on, id) = 1
)
SELECT
    CASE WHEN discount_pct >= 0.33 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    SUM(discount_pct) FILTER (WHERE promo_code = 'VIP') AS vip_total_discount_pct,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS discount_pct_per_store,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    COUNT_IF(promo_code = 'BUNDLE') AS bundle_events,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles
FROM deduped
GROUP BY CASE WHEN discount_pct >= 0.33 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY bundle_events DESC;
