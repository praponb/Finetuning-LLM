WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        tier_prices,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE effective_from > '2024-03-01'
)
-- Window metrics over several different partitions
SELECT
    id,
    effective_from,
    LAG(entry_price, 1) OVER (ORDER BY lowest_price, id) AS prev_entry_price,
    NTILE(5) OVER (PARTITION BY MONTH(effective_from) ORDER BY entry_price DESC, id) AS entry_price_bucket,
    NTILE(4) OVER (ORDER BY lowest_price DESC, id) AS lowest_price_bucket,
    SUM(entry_price) OVER (ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_entry_price,
    LAG(lowest_price, 2) OVER (PARTITION BY YEAR(effective_from) ORDER BY effective_from, id) AS prev_lowest_price
FROM flattened_attrs;
