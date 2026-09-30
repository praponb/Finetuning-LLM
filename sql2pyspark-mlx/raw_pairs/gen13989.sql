SELECT
    promo_code,
    COUNT(*) FILTER (WHERE channel = 'mobile') AS mobile_rows,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    COUNT_IF(channel = 'web') AS web_events
FROM promotions
GROUP BY promo_code
