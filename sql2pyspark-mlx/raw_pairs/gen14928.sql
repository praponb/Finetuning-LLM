WITH exploded AS (
    SELECT id, element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS element
),
filtered AS (
    SELECT *, LENGTH(element) AS element_length
    FROM exploded
    WHERE element IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        COALESCE(element, 'Unknown') AS element_filled,
        COALESCE(element, 'N/A') AS resolved_element
    FROM filtered
    WHERE element != 'WELCOME5'
),
base AS (
    SELECT
        *,
        UPPER(element) AS element_upper,
        COALESCE(element, 'NONE') AS element_clean
    FROM cleaned
    WHERE element IS DISTINCT FROM 'WELCOME5'
      AND element_length < 43
)
SELECT
    id,
    element,
    element_length,
    element_filled,
    resolved_element,
    element_upper
FROM base;
