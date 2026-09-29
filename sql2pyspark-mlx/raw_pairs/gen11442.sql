WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(coupon_codes) AS code
    FROM carts
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(code) AS code_upper
    FROM lines
    WHERE code IS DISTINCT FROM 'BUNDLE'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(code)) AS code_title,
        COALESCE(code, 'N/A') AS code_clean
    FROM base
    WHERE code = 'VIP'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        code,
        code_upper,
        code_clean,
        TRIM(code) AS clean_code,
        COALESCE(code, 'N/A') AS resolved_code
    FROM filtered
    WHERE code IN ('WELCOME5', 'SPRING10')
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(code) AS code_upper_2,
        COALESCE(code, 'N/A') AS code_clean_2
    FROM valid
    WHERE code IS DISTINCT FROM 'BUNDLE'
      AND code IS NOT NULL
)
SELECT
    code,
    code_upper,
    code_clean,
    clean_code,
    code_upper_2,
    code_clean_2
FROM valid_carts
ORDER BY code NULLS LAST;
