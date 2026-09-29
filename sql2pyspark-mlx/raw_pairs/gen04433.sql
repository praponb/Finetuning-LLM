WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS element
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(plan) AS clean_plan
    FROM flattened
    WHERE YEAR(effective_from) = 2023
),
remaining_keys AS (
    -- Set difference on keys
    SELECT plan, id
    FROM cleaned
    EXCEPT
    SELECT plan, id
    FROM price_tiers
    WHERE plan != 'basic'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(plan, 1, 2) AS plan_prefix
    FROM verified
    WHERE MONTH(effective_from) IN (2, 3, 7)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', effective_from) AS week_start,
        COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count,
        AVG(element) FILTER (WHERE plan = 'enterprise') AS enterprise_avg_element,
        ROUND(SUM(element) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS element_per_starter
    FROM valid
    GROUP BY DATE_TRUNC('WEEK', effective_from)
)
SELECT
    week_start,
    basic_count,
    enterprise_avg_element,
    element_per_starter
FROM summary
