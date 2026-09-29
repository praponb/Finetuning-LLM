WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT created_at, EXPLODE_OUTER(coupon_codes) AS value_2
    FROM flattened
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN value_2 = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5,
        TRIM(value_2) AS clean_value_2,
        COALESCE(value_2, 'N/A') AS resolved_value_2
    FROM lines
    WHERE created_at > TO_DATE('2025-03-02')
      AND value_2 IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, SUM(is_welcome5) OVER (PARTITION BY value_2) AS value_2_sum_is_welcome5
    FROM cleaned
)
SELECT
    created_at,
    value_2,
    is_welcome5,
    clean_value_2,
    value_2_sum_is_welcome5
FROM enriched
ORDER BY created_at NULLS LAST
