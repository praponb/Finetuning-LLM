WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        currency,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    plan,
    entry_price,
    AVG(entry_price) OVER w_part AS plan_avg_entry_price,
    LAG(entry_price, 1) OVER (PARTITION BY plan ORDER BY entry_price, id) AS prev_entry_price,
    AVG(entry_price) OVER (PARTITION BY plan ORDER BY entry_price, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS entry_price_avg_3_prior,
    NTILE(3) OVER w_recent AS entry_price_bucket,
    FIRST_VALUE(entry_price) IGNORE NULLS OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_entry_price
FROM flattened_attrs
WINDOW w_part AS (PARTITION BY plan), w_recent AS (PARTITION BY plan ORDER BY entry_price DESC, id)
