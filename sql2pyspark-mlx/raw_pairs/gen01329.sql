WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.credit_limit,
        c.age
    FROM flattened src
    INNER JOIN customers c
        ON src.customer_id <=> c.id
    WHERE c.is_active = TRUE
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.credit_limit AS customer_credit_limit,
        p.segment
    FROM with_dims s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.credit_limit,
        s.age,
        s.customer_credit_limit,
        p.segment AS customer_segment,
        p.country
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        country,
        COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
        COUNT(*) FILTER (WHERE customer_segment = 'Consumer') AS consumer_rows,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE customer_segment = 'SMB') AS smb_rows,
        PERCENTILE_APPROX(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles
    FROM joined
    GROUP BY country
)
SELECT
    country,
    sg_count,
    cnt
FROM summary
ORDER BY country DESC;
