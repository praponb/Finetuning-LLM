WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, v
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY created_at DESC, id, key) AS seq_no
    FROM lines
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY created_at DESC, id, key) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(key) AS key_length,
        CAST(created_at AS DATE) AS created_at_day
    FROM ranked
    WHERE created_at > DATE '2025-03-01'
      AND key IS NOT NULL
)
SELECT
    id,
    created_at,
    coupon_codes,
    key,
    seq_no,
    key_length,
    created_at_day
FROM valid
ORDER BY key DESC
