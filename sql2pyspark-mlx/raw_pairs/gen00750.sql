WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        tier_limits,
        tier_prices,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    effective_from,
    entry_price,
    NTILE(10) OVER (ORDER BY entry_price ASC, id ASC) entry_price_bucket,
    NTILE(4) OVER (ORDER BY entry_price DESC, id ASC) entry_price_bucket_2,
    RANK() OVER (ORDER BY entry_price ASC NULLS LAST) entry_price_rank
FROM flattened_attrs;
