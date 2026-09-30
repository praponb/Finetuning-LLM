WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, b.segment
    FROM exploded s
    INNER JOIN customers b
        ON s.customer_id = b.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.email,
        b.city
    FROM enriched e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN segment IN ('Consumer', 'Corporate') THEN 'Priority' ELSE 'Other' END AS segment_group,
        CASE WHEN value = 'VIP' THEN 1 ELSE 0 END AS is_vip
    FROM joined
    WHERE created_at >= TIMESTAMP '2025-03-02 20:00:00'
      AND value IN ('WELCOME5', 'BUNDLE')
)
SELECT
    id,
    customer_id,
    created_at,
    value,
    segment,
    city,
    segment_group,
    is_vip
FROM filtered
ORDER BY id DESC
