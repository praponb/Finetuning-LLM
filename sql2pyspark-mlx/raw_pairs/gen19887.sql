WITH flattened AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, value
),
flattened_carts AS (
    SELECT
        id,
        key,
        EXPLODE(coupon_codes) AS code
    FROM flattened
),
cleaned AS (
    SELECT
        *,
        UPPER(code) AS code_upper,
        COALESCE(code, 'Unknown') AS code_filled
    FROM flattened_carts
    WHERE key IS NOT NULL
      AND code <> 'WELCOME5'
),
filtered AS (
    SELECT
        key,
        code,
        code_upper,
        INITCAP(TRIM(key)) AS key_title,
        LENGTH(key) AS key_length,
        COALESCE(key, 'NONE') AS resolved_key
    FROM cleaned
    WHERE key IN ('utm_source', 'device')
),
cleaned_carts AS (
    SELECT *, COALESCE(code, 'N/A') AS code_filled
    FROM filtered
    WHERE code IS NOT NULL
),
summary AS (
    SELECT
        CASE WHEN key_length >= 74 THEN 'LARGE' ELSE 'SMALL' END AS key_length_tier,
        code,
        key,
        MAX(key_length) AS max_key_length,
        APPROX_PERCENTILE(key_length, ARRAY(0.25, 0.5, 0.75)) AS key_length_quartiles
    FROM cleaned_carts
    GROUP BY key_length_tier, code, key
)
SELECT
    key_length_tier,
    code,
    key,
    max_key_length,
    key_length_quartiles
FROM summary
ORDER BY key_length_tier DESC NULLS FIRST, key DESC;
