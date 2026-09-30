WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, attr_value
),
exploded_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        code
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) li AS code
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, c.age
    FROM exploded_carts src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.signup_date,
        c.age AS customer_age
    FROM enriched s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, p.country
    FROM joined e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM with_dims
    EXCEPT ALL
    SELECT customer_id, id
    FROM carts
    WHERE created_at < '2024-05-25'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        country,
        CASE WHEN customer_age >= 57 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_age_tier,
        ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN k = 'utm_source' THEN 1 END), 0), 2) AS age_per_utm_source,
        ROUND(SUM(customer_age) / NULLIF(COUNT(CASE WHEN country = 'TH' THEN 1 END), 0), 2) AS customer_age_per_th,
        SORT_ARRAY(COLLECT_SET(code)) AS code_list,
        COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
        RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', signup_date) ORDER BY ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN k = 'utm_source' THEN 1 END), 0), 2) DESC) AS age_per_utm_source_rank
    FROM reconciled
    GROUP BY 1, 2, 3
)
SELECT
    quarter_start,
    country,
    customer_age_tier,
    age_per_utm_source,
    customer_age_per_th,
    code_list,
    uk_count
FROM stats
ORDER BY customer_age_per_th DESC;
