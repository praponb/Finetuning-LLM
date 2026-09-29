WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, currency
    FROM exploded
    EXCEPT
    SELECT id, currency
    FROM price_tiers
    WHERE plan = 'enterprise'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN remaining_keys k ON s.id = k.id AND s.currency = k.currency
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN tier_value >= 69 THEN 'High' WHEN tier_value >= 20 THEN 'Medium' ELSE 'Low' END AS tier_value_band
    FROM reconciled
    WHERE tier_value BETWEEN 54 AND 74
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(currency) AS currency_length
    FROM filtered
    WHERE tier_value <= 67
)
-- Grouped metrics
SELECT
    currency,
    CASE WHEN currency_length >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_length_tier,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    ROUND(AVG(currency_length), 2) AS avg_currency_length
FROM valid
GROUP BY GROUPING SETS ((currency, CASE WHEN currency_length >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END), (currency), ());
