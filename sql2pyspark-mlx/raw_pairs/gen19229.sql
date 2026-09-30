WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attributes,
        COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default,
        TRANSFORM(items, i -> i.product_id) AS product_ids
    FROM carts
    WHERE customer_id IN (6, 19, 20)
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    utm_source_or_default,
    FIRST_VALUE(utm_source_or_default, TRUE) OVER (ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_utm_source_or_default,
    FIRST_VALUE(utm_source_or_default) IGNORE NULLS OVER (PARTITION BY TO_DATE(created_at) ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_utm_source_or_default_2,
    ROW_NUMBER() OVER (PARTITION BY MONTH(created_at), customer_id ORDER BY created_at, id) AS seq_no
FROM parsed;
