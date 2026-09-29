WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
    WHERE plan = 'starter'
)
SELECT
    id,
    plan,
    effective_from,
    tier_limits,
    first_two_limits,
    avg_tier_price,
    lowest_price,
    AVG(avg_tier_price) OVER (PARTITION BY plan) AS plan_avg_avg_tier_price,
    LEAD(avg_tier_price, 1) OVER (ORDER BY avg_tier_price, id) AS next_avg_tier_price,
    SUM(avg_tier_price) OVER (PARTITION BY plan ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_avg_tier_price,
    SUM(lowest_price) OVER (PARTITION BY plan ORDER BY avg_tier_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_lowest_price,
    SUM(avg_tier_price) OVER (PARTITION BY plan ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_tier_price
FROM flattened_attrs;
