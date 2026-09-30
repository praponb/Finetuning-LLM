WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.customer_id,
        c.created_at,
        c.coupon_codes,
        line.product_id
    FROM carts c
    LATERAL VIEW OUTER EXPLODE(c.items) e AS line
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.product_id,
        d.email,
        d.signup_date,
        d.segment
    FROM flattened a
    INNER JOIN customers d
        ON a.customer_id <=> d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, p.credit_limit
    FROM with_dims src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.product_id,
        a.email,
        a.segment,
        p.age,
        p.name
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        customer_id,
        product_id,
        email,
        segment,
        age,
        name,
        TRIM(email) AS clean_email,
        COALESCE(segment, 'N/A') AS segment_filled,
        COALESCE(segment, 'OTHER') AS resolved_segment
    FROM joined
    WHERE age <= 41
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY customer_id), 4) AS product_id_share,
        ROUND(age / SUM(age) OVER (PARTITION BY customer_id), 4) AS age_share
    FROM filtered
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN age_share >= 0.78 THEN 'HIGH' ELSE 'LOW' END AS age_share_tier,
        COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
        COUNT(*) AS num_rows
    FROM with_windows
    GROUP BY 1
)
SELECT
    age_share_tier,
    consumer_rows,
    num_rows
FROM grouped;
