WITH parsed AS (
    SELECT
        id,
        customer_id,
        created_at,
        ship_to,
        attributes,
        attributes['utm_source'] AS utm_source,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS used_bundle
    FROM carts
)
SELECT
    id,
    created_at,
    utm_source,
    used_bundle,
    ROW_NUMBER() OVER (ORDER BY created_at, id) AS event_seq,
    LAST_VALUE(utm_source) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_utm_source,
    FIRST_VALUE(utm_source) IGNORE NULLS OVER (ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_utm_source
FROM parsed
WHERE TO_DATE(created_at) < '2025-02-19'
