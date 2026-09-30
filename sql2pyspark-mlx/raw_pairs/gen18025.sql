WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE(discounts) AS element
    FROM price_tiers
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(element, 2) AS element_rounded,
        CEIL(element) AS element_whole
    FROM flattened
    WHERE currency IN ('THB', 'EUR')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        currency,
        element_rounded,
        element_whole,
        CASE WHEN element >= 88 THEN 'High' WHEN element >= 32 THEN 'Medium' ELSE 'Low' END AS element_band
    FROM cleaned
    WHERE element < 46
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(element_rounded) AS element_rounded_whole,
        CASE WHEN currency IN ('THB', 'USD') THEN 'Priority' ELSE 'Other' END AS currency_group
    FROM filtered
    WHERE currency IN ('THB', 'EUR', 'USD')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        currency,
        element_rounded,
        element_band,
        CUME_DIST() OVER w_running AS element_whole_cume_dist
    FROM base
    WHERE element_rounded_whole < 63
    WINDOW w_running AS (ORDER BY element_whole DESC NULLS FIRST)
)
SELECT
    currency,
    element_band,
    CASE WHEN currency RLIKE '^(EUR|THB)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS currency_zone
FROM enriched
ORDER BY currency NULLS FIRST, currency_zone;
