WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        plan,
        currency,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    plan,
    COUNT(entry_price) OVER (PARTITION BY currency, plan ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_entry_price,
    AVG(entry_price) OVER (PARTITION BY currency, plan ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_entry_price,
    MAX(entry_price) OVER (PARTITION BY currency, plan ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_entry_price,
    SUM(entry_price) OVER (PARTITION BY currency, plan ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_entry_price
FROM unpacked
