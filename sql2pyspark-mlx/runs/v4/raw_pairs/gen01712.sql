WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(value, 'unassigned') AS value_filled
    FROM flattened
    WHERE customer_id = 37
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(value, 1, 3) AS value_prefix
    FROM valid
    WHERE value IN ('VIP', 'SPRING10')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.email,
        d.segment,
        d.age
    FROM base a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        value,
        email,
        segment,
        age,
        CONCAT(email, '/', segment) AS email_segment,
        COALESCE(value, 'NONE') AS value_filled_2
    FROM with_dims
    WHERE customer_id IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(segment) AS segment_upper
    FROM valid_carts
    WHERE segment IS NOT NULL
      AND age <> 56
)
-- Aggregate with conditional metrics
SELECT
    segment,
    CASE WHEN age >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    SORT_ARRAY(COLLECT_SET(value)) AS value_list,
    PERCENTILE_APPROX(age, 0.9) AS age_p90,
    SIZE(COLLECT_SET(value)) AS num_values
FROM cleaned
GROUP BY segment, age_tier
ORDER BY age_tier DESC
