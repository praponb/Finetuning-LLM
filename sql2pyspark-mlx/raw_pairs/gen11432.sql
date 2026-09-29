WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        value,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM lines
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        k,
        value,
        tag,
        CONCAT_WS('-', k, tag) AS k_tag_key,
        COALESCE(tag, 'OTHER') AS resolved_tag
    FROM flattened
    WHERE k IN ('utm_source', 'device')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        k,
        tag,
        k_tag_key,
        resolved_tag,
        CONCAT(k, ' ', tag) AS k_tag,
        COALESCE(tag, 'UNKNOWN') AS resolved_tag_2
    FROM valid
    WHERE tag IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.country,
        p.city
    FROM cleaned src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
)
SELECT
    id,
    customer_id,
    k,
    tag,
    resolved_tag,
    k_tag,
    resolved_tag_2,
    country,
    city,
    CASE WHEN country RLIKE '^(DE|JP)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS country_zone
FROM enriched;
