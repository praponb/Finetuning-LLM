WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(coupon_codes) AS value
    FROM carts
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM flattened
    EXCEPT
    SELECT id
    FROM carts
    WHERE created_at >= '2025-06-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(value)) AS value_title
    FROM kept_rows
    WHERE value = 'VIP'
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN value IN ('WELCOME5', 'SPRING10') THEN 'Core' ELSE 'Other' END AS value_group,
        COALESCE(value, 'OTHER') AS value_filled
    FROM filtered
    WHERE value IS NOT NULL
      AND value IN ('WELCOME5', 'SPRING10', 'BUNDLE')
)
SELECT
    id,
    value_title,
    value_group,
    value_filled
FROM filtered_carts
ORDER BY id DESC
