WITH parsed AS (
    SELECT
        id,
        currency,
        tier_limits,
        tier_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE currency IN ('THB', 'EUR', 'USD')
)
SELECT
    id,
    avg_tier_price,
    FIRST_VALUE(avg_tier_price) IGNORE NULLS OVER (ORDER BY avg_tier_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_avg_tier_price,
    SUM(avg_tier_price) OVER (ORDER BY avg_tier_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_tier_price,
    LEAD(avg_tier_price, 1, 0) OVER (PARTITION BY currency ORDER BY avg_tier_price, id) AS next_avg_tier_price,
    AVG(avg_tier_price) OVER (PARTITION BY currency ORDER BY avg_tier_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_tier_price_avg_2
FROM parsed
WHERE currency = 'USD'
