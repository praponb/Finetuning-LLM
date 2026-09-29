WITH flattened_attrs AS (
    SELECT
        id,
        tier_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
SELECT
    id,
    tier_limits,
    entry_price,
    tier_revenue_caps,
    AVG(entry_price) OVER (ORDER BY entry_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS entry_price_avg_5,
    PERCENT_RANK() OVER w_ordered AS entry_price_percent_rank,
    ROW_NUMBER() OVER w_ordered_2 AS seq_no,
    NTILE(4) OVER w_ordered_2 AS entry_price_bucket,
    ROW_NUMBER() OVER w_ordered_2 AS event_seq
FROM flattened_attrs
WINDOW w_ordered AS (ORDER BY entry_price DESC NULLS LAST), w_ordered_2 AS (ORDER BY entry_price, id);
