WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE effective_from < '2025-01-01'
)
-- Window metrics over several different partitions
SELECT
    id,
    plan,
    effective_from,
    entry_price,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY avg_tier_price ASC, id ASC) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY effective_from ASC, id ASC) AS seq_no,
    NTILE(4) OVER (PARTITION BY plan ORDER BY avg_tier_price ASC, id ASC) AS avg_tier_price_bucket
FROM flattened_attrs;
