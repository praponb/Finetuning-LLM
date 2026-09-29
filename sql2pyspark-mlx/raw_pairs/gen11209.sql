WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        effective_from,
        currency,
        element,
        FLOOR(element) AS element_whole,
        SUBSTRING(currency, 1, 3) AS currency_prefix
    FROM exploded
    WHERE effective_from > TO_DATE('2024-02-02')
      AND effective_from >= '2023-04-19'
),
kept_keys AS (
    -- Set difference on keys
    SELECT currency
    FROM valid
    EXCEPT
    SELECT currency
    FROM price_tiers
    WHERE currency IN ('EUR', 'USD', 'THB')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.currency = k.currency
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT currency
    FROM kept_rows
    INTERSECT ALL
    SELECT currency
    FROM price_tiers
    WHERE plan NOT IN ('basic', 'starter', 'enterprise')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys_2 k ON s.currency = k.currency
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DAY(effective_from) AS effective_from_day
    FROM reconciled
    WHERE currency IN ('USD', 'EUR')
)
SELECT
    effective_from,
    currency,
    element_whole,
    CASE WHEN currency RLIKE '^(EUR|USD)$' THEN 'GROUP_A' ELSE 'CORE' END AS currency_zone
FROM cleaned
ORDER BY currency_zone DESC NULLS LAST, effective_from
