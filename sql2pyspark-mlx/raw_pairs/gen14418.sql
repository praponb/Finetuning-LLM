WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        discounts,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-10-01' AND '2024-01-01'
)
-- Ranking and gap features
SELECT
    id,
    effective_from,
    STDDEV_SAMP(avg_tier_price) OVER (PARTITION BY currency) AS currency_stddev_samp_avg_tier_price,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY avg_tier_price) AS avg_tier_price_percent_rank
FROM parsed;
