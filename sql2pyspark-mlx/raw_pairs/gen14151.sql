WITH parsed AS (
    SELECT
        id,
        effective_from,
        tier_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
SELECT
    *,
    AVG(lowest_price) OVER (ORDER BY lowest_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS lowest_price_avg_6_prior,
    NTILE(4) OVER (ORDER BY lowest_price ASC, id ASC) AS lowest_price_bucket,
    ROW_NUMBER() OVER w_grp AS seq_no
FROM parsed
WINDOW w_grp AS (ORDER BY effective_from DESC, id ASC)
