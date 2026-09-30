WITH unpacked AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE effective_from BETWEEN '2024-12-31' AND '2025-08-30'
)
SELECT
    *,
    AVG(entry_price) OVER (PARTITION BY plan ORDER BY effective_from ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS entry_price_avg_5,
    STDDEV_SAMP(avg_tier_price) OVER w_grp AS currency_stddev_samp_avg_tier_price,
    FIRST_VALUE(avg_tier_price, TRUE) OVER w_grp_2 AS first_known_avg_tier_price,
    AVG(entry_price) OVER w_hist AS entry_price_avg_6_prior
FROM unpacked
WINDOW w_grp AS (PARTITION BY currency), w_grp_2 AS (PARTITION BY YEAR(effective_from), plan ORDER BY avg_tier_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY YEAR(effective_from), currency ORDER BY entry_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING);
