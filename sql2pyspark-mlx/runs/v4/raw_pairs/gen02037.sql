WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        plan,
        tier_limits,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN entry_price >= 34 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    CASE WHEN lowest_price >= 30 THEN 'LARGE' ELSE 'SMALL' END AS lowest_price_tier,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    APPROX_PERCENTILE(entry_price, 0.95) AS entry_price_p95,
    SUM(CASE WHEN plan = 'starter' THEN lowest_price ELSE 0 END) AS starter_lowest_price,
    PERCENTILE_APPROX(entry_price, 0.95) AS entry_price_p95_2,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN entry_price >= 34 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY APPROX_PERCENTILE(entry_price, 0.95) DESC) AS entry_price_p95_percent_rank
FROM flattened_attrs
GROUP BY CASE WHEN entry_price >= 34 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN lowest_price >= 30 THEN 'LARGE' ELSE 'SMALL' END;
