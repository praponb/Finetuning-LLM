WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS val
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(currency) AS currency_lower
    FROM flattened
    WHERE currency IN ('EUR', 'USD', 'THB')
),
scored AS (
    -- Ranking and gap features
    SELECT *, AVG(val) OVER w_recent AS currency_avg_val
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY currency)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency_avg_val,
        LEAST(currency_avg_val, val) AS least_currency_avg_val_val
    FROM scored
    WHERE val = 37
)
-- Multi-level aggregation
SELECT
    CASE WHEN currency_avg_val >= 82 THEN 'HIGH' ELSE 'LOW' END AS currency_avg_val_tier,
    MIN(least_currency_avg_val_val) AS min_least_currency_avg_val_val,
    COUNT(*) AS row_count
FROM valid
GROUP BY ROLLUP (CASE WHEN currency_avg_val >= 82 THEN 'HIGH' ELSE 'LOW' END)
HAVING min_least_currency_avg_val_val > 0
ORDER BY min_least_currency_avg_val_val;
