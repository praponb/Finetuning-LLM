WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, v
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        v,
        EXPLODE(coupon_codes) AS code
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label,
        COALESCE(code, 'OTHER') AS code_filled
    FROM lines_carts
    WHERE code IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.created_at,
        s.v,
        s.code,
        s.code_filled,
        d.name
    FROM valid s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        created_at,
        code,
        code_filled,
        name,
        WEEKOFYEAR(created_at) AS created_at_weekofyear,
        CAST(created_at AS DATE) AS created_at_day,
        COALESCE(code, 'DIRECT') AS resolved_code
    FROM enriched
    WHERE created_at > TIMESTAMP '2025-03-01 23:00:00'
      AND HOUR(created_at) BETWEEN 9 AND 14
)
SELECT
    s.customer_id,
    s.created_at,
    s.code,
    s.name,
    s.created_at_day,
    CASE WHEN name RLIKE '^(Noah Park|Grace Kim)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS name_zone,
    (SELECT MIN(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM cleaned s
ORDER BY customer_id DESC NULLS LAST, code DESC
