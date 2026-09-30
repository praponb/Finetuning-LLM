WITH parsed AS (
    SELECT
        id,
        effective_from,
        tier_limits,
        tier_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    CASE WHEN avg_tier_price >= 30 THEN 'PREMIUM' ELSE 'STANDARD' END AS avg_tier_price_tier,
    APPROX_PERCENTILE(lowest_price, 0.5) AS lowest_price_p50,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    COUNT(*) AS event_count
FROM parsed
GROUP BY CUBE (DATE_TRUNC('WEEK', effective_from), CASE WHEN avg_tier_price >= 30 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY cnt;
