WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(plan)) AS plan_title
    FROM lines
    WHERE plan NOT IN ('pro', 'basic', 'starter')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(currency, ' - ', plan) AS currency_plan
    FROM cleaned
    WHERE plan IN ('basic', 'enterprise', 'starter')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        currency,
        element,
        plan_title,
        SUBSTRING(plan, 1, 4) AS plan_prefix
    FROM valid
    WHERE element < 55
)
-- Grouped metrics
SELECT
    CASE WHEN element >= 67 THEN 'HIGH' ELSE 'LOW' END AS element_tier,
    currency,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    SUM(element) AS total_element
FROM filtered
GROUP BY CASE WHEN element >= 67 THEN 'HIGH' ELSE 'LOW' END, currency
HAVING SUM(element) > 0
ORDER BY num_currencies NULLS FIRST
