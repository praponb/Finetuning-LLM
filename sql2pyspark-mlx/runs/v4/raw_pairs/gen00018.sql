SELECT
    CASE WHEN discount_pct >= 0.36 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    DATE_TRUNC('WEEK', starts_on) AS week_start,
    COUNT_IF(channel = 'store') AS store_events,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT channel) AS distinct_channels,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    AVG(discount_pct) FILTER (WHERE promo_code = 'SPRING10') AS spring10_avg_discount_pct,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN discount_pct >= 0.36 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT_IF(channel = 'store') DESC) AS store_events_dense_rank
FROM promotions
GROUP BY discount_pct_tier, week_start
ORDER BY week_start ASC NULLS FIRST
