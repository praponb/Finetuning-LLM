WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, code
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) x AS code
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN code IN ('VIP', 'SPRING10') THEN 'Priority' ELSE 'Other' END AS code_group
    FROM lines
    WHERE code IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(code, 1, 3) AS code_prefix,
        COALESCE(code, 'NONE') AS code_clean
    FROM valid
    WHERE code IN ('BUNDLE', 'WELCOME5', 'VIP')
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        code_group,
        code_prefix,
        code_clean,
        LOWER(code) AS code_lower,
        CASE WHEN code = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5,
        COALESCE(code, 'OTHER') AS code_filled
    FROM cleaned
    WHERE code NOT IN ('SPRING10', 'WELCOME5')
)
SELECT
    id,
    code_group,
    code_lower,
    is_welcome5,
    code_filled
FROM cleaned_carts
ORDER BY is_welcome5
