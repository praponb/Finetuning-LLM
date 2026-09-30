WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.qty,
        line.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS line
        FROM carts
    ) t
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        promo,
        EXPLODE(coupon_codes) AS value
    FROM lines
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN promo IN ('BUNDLE', 'VIP') THEN 'Tier 1' ELSE 'Other' END AS promo_group
    FROM flattened
    WHERE value IN ('BUNDLE', 'WELCOME5')
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(promo) AS clean_promo,
        COALESCE(value, 'OTHER') AS value_filled
    FROM valid
    WHERE promo IS NOT NULL
      AND value IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(value) AS value_upper,
        TRIM(promo) AS clean_promo_2,
        COALESCE(value, 'DIRECT') AS resolved_value
    FROM valid_carts
    WHERE created_at >= DATE '2025-03-02'
      AND value IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        created_at,
        promo,
        value,
        value_filled,
        clean_promo_2,
        SUBSTRING(promo, 1, 4) AS promo_prefix,
        COALESCE(value, 'UNKNOWN') AS value_filled_2
    FROM base
    WHERE created_at >= TO_DATE('2025-03-01')
      AND value IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        created_at,
        promo,
        value,
        value_filled,
        clean_promo_2,
        promo_prefix,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM cleaned
    WHERE promo IS NOT NULL
)
SELECT
    created_at,
    value_filled,
    clean_promo_2,
    promo_prefix,
    days_since_created_at
FROM filtered
ORDER BY created_at;
