SELECT
    DATE_TRUNC('YEAR', ends_on) AS year_start,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    MAX(discount_pct) FILTER (WHERE promo_code = 'VIP') AS vip_max_discount_pct,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    SUM(CASE WHEN promo_code = 'WELCOME5' THEN discount_pct ELSE 0 END) AS welcome5_discount_pct,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    MAX(discount_pct) AS max_discount_pct
FROM promotions
GROUP BY DATE_TRUNC('YEAR', ends_on);
