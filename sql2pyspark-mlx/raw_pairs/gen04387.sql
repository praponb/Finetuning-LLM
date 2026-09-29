WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(key) AS clean_key,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS is_spring10,
        COALESCE(key, 'OTHER') AS resolved_key
    FROM exploded
    WHERE key IS NOT NULL
      AND key IN ('weight_kg', 'gift', 'device')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        coupon_codes,
        key,
        attr_value,
        clean_key,
        is_spring10,
        resolved_key,
        LOWER(key) AS key_lower
    FROM filtered
    WHERE key IS NOT NULL
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        IF(is_spring10, 'Yes', 'No') AS is_spring10_label,
        COALESCE(key, 'N/A') AS resolved_key_2
    FROM cleaned
    WHERE key IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(key, 'Unknown') AS key_filled
    FROM filtered_carts
    WHERE key IS NOT NULL
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT *, CAST(is_spring10 AS INT) AS is_spring10_flag
    FROM valid
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
      AND key IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    coupon_codes,
    key,
    key_lower,
    resolved_key_2,
    is_spring10_flag,
    ROW_NUMBER() OVER w_running AS row_seq,
    LAG(is_spring10_flag, 1) OVER (PARTITION BY key ORDER BY is_spring10_flag, id, key) AS prev_is_spring10_flag,
    ROW_NUMBER() OVER w_running AS row_seq_2,
    SUM(is_spring10_flag) OVER (PARTITION BY key ORDER BY is_spring10_flag, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_spring10_flag,
    ROW_NUMBER() OVER (ORDER BY is_spring10_flag DESC, id, key) AS row_seq_3
FROM valid_carts
WHERE is_spring10 = TRUE
WINDOW w_running AS (PARTITION BY key ORDER BY is_spring10_flag DESC, id, key)
