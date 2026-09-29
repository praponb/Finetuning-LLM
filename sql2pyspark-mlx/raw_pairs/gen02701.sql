WITH parsed AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE effective_from BETWEEN '2024-05-01' AND '2025-01-01'
)
SELECT
    id,
    effective_from,
    NTILE(4) OVER (PARTITION BY currency ORDER BY entry_price DESC, id ASC) entry_price_bucket,
    NTILE(4) OVER (PARTITION BY currency ORDER BY entry_price DESC, id ASC) entry_price_bucket_2,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY entry_price DESC, id ASC) row_seq
FROM parsed
