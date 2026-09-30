WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS element
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_recent element_rank,
        AVG(element) OVER w_part currency_avg_element
    FROM flattened
    WINDOW w_recent AS (PARTITION BY currency ORDER BY element NULLS LAST), w_part AS (PARTITION BY currency)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        element,
        currency_avg_element,
        COALESCE(element_rank, 0) element_rank_filled,
        CAST(currency_avg_element AS STRING) currency_avg_element_num
    FROM with_windows
    WHERE currency = 'USD'
)
-- Grouped metrics
SELECT
    CASE WHEN currency_avg_element >= 88 THEN 'HIGH' ELSE 'LOW' END AS currency_avg_element_tier,
    APPROX_PERCENTILE(element_rank_filled, ARRAY(0.25, 0.5, 0.75)) AS element_rank_filled_quartiles,
    SUM(CASE WHEN currency = 'USD' THEN element_rank_filled ELSE 0 END) AS usd_element_rank_filled,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows
FROM cleaned
GROUP BY CUBE (CASE WHEN currency_avg_element >= 88 THEN 'HIGH' ELSE 'LOW' END)
HAVING usd_element_rank_filled < 0
ORDER BY eur_rows DESC NULLS FIRST
