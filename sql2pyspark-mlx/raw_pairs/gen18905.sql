WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, v
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        ARRAY_JOIN(coupon_codes, ', ') coupon_codes_text
    FROM flattened
    WHERE k = 'gift'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.k,
        a.coupon_codes_text,
        b.segment
    FROM valid a
    LEFT JOIN customers b
        ON a.customer_id = b.id
    WHERE b.country != 'TH'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        k,
        coupon_codes_text,
        segment,
        CASE WHEN k IN ('device', 'utm_source') THEN 'Priority' ELSE 'Other' END k_group,
        COALESCE(segment, 'DIRECT') segment_clean
    FROM joined
    WHERE segment = 'SMB'
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.k,
        s.k_group,
        s.segment_clean,
        b.city,
        b.segment AS customer_segment,
        b.country
    FROM filtered s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        k,
        customer_segment,
        UPPER(city) city_upper,
        COALESCE(customer_segment, 'NONE') customer_segment_clean
    FROM joined_carts
    WHERE customer_id IS NOT NULL
      AND city IS NOT DISTINCT FROM 'Singapore'
)
SELECT
    id,
    customer_id,
    k,
    customer_segment,
    city_upper
FROM filtered_carts;
