WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(value, 'Unknown') value_filled
    FROM lines
    WHERE created_at > TIMESTAMP '2025-03-01 20:00:00'
      AND value IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAYOFWEEK(created_at) created_at_dayofweek,
        TRIM(value) clean_value
    FROM valid
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 13:00:00')
      AND value IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(created_at_dayofweek, 0) created_at_dayofweek_filled,
        CASE WHEN value IN ('WELCOME5', 'SPRING10') THEN 'Group A' ELSE 'Other' END value_group
    FROM base
    WHERE value = 'VIP'
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, created_at_dayofweek_filled - created_at_dayofweek created_at_dayofweek_filled_minus_created_at_dayofweek
    FROM filtered
    WHERE value IS NOT NULL
)
SELECT
    id,
    created_at,
    value,
    value_filled,
    created_at_dayofweek,
    clean_value,
    created_at_dayofweek_filled,
    value_group,
    created_at_dayofweek_filled_minus_created_at_dayofweek
FROM base_carts
ORDER BY created_at_dayofweek_filled;
