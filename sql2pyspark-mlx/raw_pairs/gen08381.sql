WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS element
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM lines
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE plan NOT IN ('enterprise', 'pro')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        effective_from,
        currency,
        element,
        CONCAT(currency, ' - ', plan) AS currency_plan
    FROM kept_rows
    WHERE effective_from >= TO_DATE('2024-05-13')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(effective_from, 72) AS effective_from_plus_72d
    FROM valid
    WHERE element >= 57
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT currency
    FROM filtered
    EXCEPT DISTINCT
    SELECT currency
    FROM price_tiers
    WHERE effective_from > '2025-03-01'
),
kept_rows_price_tiers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys_2 k ON s.currency = k.currency
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(effective_from, 1) AS effective_from_plus_months
    FROM kept_rows_price_tiers
    WHERE effective_from >= DATE '2024-04-18'
      AND effective_from >= '2023-11-13'
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_SUB(effective_from_plus_months, 39) AS effective_from_plus_months_minus_39d
    FROM cleaned
    WHERE currency IN ('EUR', 'USD')
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    currency,
    AVG(element) AS avg_element,
    COUNT(*) AS cnt,
    SUM(CASE WHEN currency = 'USD' THEN element ELSE 0 END) AS usd_element,
    COUNT(*) AS event_count
FROM valid_price_tiers
GROUP BY DATE_TRUNC('WEEK', effective_from), currency
HAVING avg_element > 0
