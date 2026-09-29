WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS element
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(element * 1.15, 2) AS adjusted_element
    FROM lines
    WHERE currency = 'THB'
),
metrics AS (
    -- Ranking and gap features
    SELECT *, MIN(adjusted_element) OVER w_ordered AS currency_min_adjusted_element
    FROM base
    WHERE element < 52
    WINDOW w_ordered AS (PARTITION BY currency)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT currency, id
    FROM metrics
    INTERSECT
    SELECT currency, id
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-08-01' AND '2024-09-24'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.currency = k.currency AND s.id = k.id
)
SELECT
    id,
    currency,
    currency_min_adjusted_element,
    CASE WHEN currency RLIKE '^(THB|USD)$' THEN 'GROUP_B' WHEN currency RLIKE '^(EUR)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS currency_zone
FROM verified
ORDER BY currency DESC
