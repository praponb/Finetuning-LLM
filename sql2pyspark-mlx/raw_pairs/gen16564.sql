WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, value
),
lines AS (
    SELECT
        id,
        created_at,
        key,
        value,
        code
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS code
),
cleaned AS (
    SELECT
        id,
        created_at,
        key,
        code,
        CONCAT_WS(',', key, code) AS key_code_key,
        SUBSTRING(key, 1, 2) AS key_prefix
    FROM lines
    WHERE key IS NOT NULL
),
base AS (
    SELECT *, SUBSTRING(code, 1, 4) AS code_prefix
    FROM cleaned
    WHERE created_at > TO_DATE('2025-03-02')
      AND key IS NOT NULL
),
base_carts AS (
    SELECT
        *,
        DAYOFWEEK(created_at) AS created_at_dayofweek,
        COALESCE(key, 'NONE') AS key_clean
    FROM base
    WHERE created_at >= DATE '2025-03-01'
      AND key IS DISTINCT FROM 'gift'
),
cleaned_carts AS (
    SELECT
        id,
        code,
        key_code_key,
        key_prefix,
        created_at_dayofweek,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        TO_DATE(created_at) AS created_at_date,
        COALESCE(key, 'UNKNOWN') AS key_clean_2
    FROM base_carts
    WHERE created_at >= DATE '2025-03-01'
      AND key IS NOT NULL
)
SELECT
    id,
    key_code_key,
    key_prefix,
    created_at_dayofweek,
    created_at_date
FROM cleaned_carts
ORDER BY created_at_date;
