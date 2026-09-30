WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS key, attr_value
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes,
        COALESCE(key, 'N/A') AS key_filled
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-01 22:00:00'
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.attr_value,
        b.country
    FROM valid src
    INNER JOIN customers b
        ON src.customer_id = b.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        created_at,
        attr_value,
        country,
        COALESCE(country, 'N/A') AS country_filled
    FROM joined
    WHERE created_at > TIMESTAMP '2025-03-02 01:00:00'
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(country) AS clean_country,
        COALESCE(country, 'NONE') AS country_clean
    FROM filtered
    WHERE country IS NOT DISTINCT FROM 'JP'
),
joined_carts AS (
    -- Attach customers attributes
    SELECT a.*, d.segment
    FROM filtered_carts a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.country AS customer_country,
        b.signup_date
    FROM joined_carts s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    id,
    created_at,
    attr_value,
    country,
    country_filled,
    clean_country,
    customer_country,
    signup_date,
    CASE WHEN customer_country RLIKE '^(TH|US)$' THEN 'GROUP_B' WHEN customer_country RLIKE '^(SG|JP)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS customer_country_zone
FROM enriched
ORDER BY signup_date DESC
