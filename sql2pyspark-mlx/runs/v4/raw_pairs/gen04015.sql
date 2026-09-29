WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        tier_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE MONTH(effective_from) IN (1, 4, 8)
)
-- Window metrics over several different partitions
SELECT
    *,
    LAG(entry_price, 2) OVER w_hist AS prev_entry_price,
    SUM(avg_tier_price) OVER (PARTITION BY plan ORDER BY avg_tier_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_avg_tier_price
FROM parsed
WINDOW w_hist AS (PARTITION BY plan ORDER BY effective_from, id)
