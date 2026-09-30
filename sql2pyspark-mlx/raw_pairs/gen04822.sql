WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS value
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        p.segment,
        p.age
    FROM lines src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(segment)) AS segment_title
    FROM joined
    WHERE customer_id IS NOT NULL
      AND segment IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.segment,
        src.segment_title,
        b.age AS customer_age,
        b.segment AS customer_segment,
        b.name
    FROM base src
    JOIN customers b
        ON src.customer_id = b.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(customer_segment, 'Unknown') AS customer_segment_filled
    FROM enriched
    WHERE customer_segment IS NOT NULL
      AND customer_id = 23
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN customer_age >= 67 THEN 'HIGH' ELSE 'LOW' END AS customer_age_tier,
    customer_id,
    SIZE(COLLECT_SET(customer_segment)) AS num_customer_segments,
    ROUND(SUM(customer_age) / NULLIF(COUNT(CASE WHEN customer_segment = 'Consumer' THEN 1 END), 0), 2) AS customer_age_per_consumer,
    COUNT(*) FILTER (WHERE customer_segment = 'SMB') AS smb_rows
FROM cleaned
GROUP BY CASE WHEN customer_age >= 67 THEN 'HIGH' ELSE 'LOW' END, customer_id;
