WITH parsed AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_prices,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE plan IN ('basic', 'pro', 'enterprise')
)
SELECT
    id,
    plan,
    effective_from,
    MAX(entry_price) OVER w_grp AS running_max_entry_price,
    MIN(entry_price) OVER w_grp AS running_min_entry_price,
    SUM(entry_price) OVER w_grp AS running_sum_entry_price,
    MAX(avg_tier_price) OVER (PARTITION BY plan) AS plan_max_avg_tier_price
FROM parsed
WINDOW w_grp AS (PARTITION BY plan ORDER BY avg_tier_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
