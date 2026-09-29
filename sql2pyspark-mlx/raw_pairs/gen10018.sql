WITH flattened AS (
    SELECT id, element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS element
),
cleaned AS (
    SELECT
        *,
        UPPER(element) AS element_upper,
        COALESCE(element, 'NONE') AS element_clean
    FROM flattened
    WHERE element IS DISTINCT FROM 'SPRING10'
),
cleaned_carts AS (
    SELECT
        *,
        TRIM(element) AS clean_element,
        COALESCE(element, 'OTHER') AS resolved_element
    FROM cleaned
    WHERE element IS NOT NULL
),
base AS (
    SELECT
        id,
        element,
        element_upper,
        element_clean,
        clean_element,
        resolved_element,
        LENGTH(element) AS element_length,
        COALESCE(element, 'DIRECT') AS element_filled
    FROM cleaned_carts
    WHERE element IS NOT NULL
)
SELECT
    element,
    COUNT_IF(element = 'WELCOME5') AS welcome5_events,
    SORT_ARRAY(COLLECT_SET(element)) AS element_list,
    COUNT_IF(element = 'BUNDLE') AS bundle_events
FROM base
GROUP BY element;
