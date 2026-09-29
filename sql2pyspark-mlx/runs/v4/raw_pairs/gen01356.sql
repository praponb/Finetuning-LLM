SELECT
    promo_code,
    channel,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM promotions
GROUP BY promo_code, channel
ORDER BY channel DESC NULLS FIRST;
