SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    CASE WHEN discount_pct >= 0.35 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    COUNT(DISTINCT channel) AS distinct_channels,
    GROUPING_ID() AS gid
FROM promotions
GROUP BY ROLLUP (DATE_TRUNC('MONTH', ends_on), CASE WHEN discount_pct >= 0.35 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY activity_month DESC;
