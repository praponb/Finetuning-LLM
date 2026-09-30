WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS element
)
-- Multi-level aggregation
SELECT
    element,
    COUNT(DISTINCT element) AS distinct_elements,
    COUNT(*) AS cnt
FROM lines
GROUP BY element
ORDER BY cnt NULLS LAST;
