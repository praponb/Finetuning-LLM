WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-01 19:00:00'
      AND code IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        WEEKOFYEAR(created_at) AS created_at_weekofyear,
        FLOOR(days_since_created_at) AS days_since_created_at_whole,
        COALESCE(code, 'NONE') AS resolved_code
    FROM base
    WHERE code IN ('WELCOME5', 'BUNDLE')
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        days_since_created_at,
        created_at_weekofyear,
        days_since_created_at_whole,
        GREATEST(days_since_created_at, created_at_weekofyear) AS greatest_days_since_created_at_created_at_weekofyear,
        COALESCE(code, 'UNKNOWN') AS resolved_code_2
    FROM filtered
    WHERE code IS NOT DISTINCT FROM 'WELCOME5'
      AND code IS NOT NULL
)
SELECT
    id,
    days_since_created_at,
    created_at_weekofyear,
    greatest_days_since_created_at_created_at_weekofyear,
    resolved_code_2
FROM filtered_carts
ORDER BY id DESC, days_since_created_at
