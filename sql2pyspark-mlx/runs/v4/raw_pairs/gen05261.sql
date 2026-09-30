WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, v
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM exploded
    EXCEPT DISTINCT
    SELECT id, customer_id
    FROM carts
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM exploded s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.age,
        c.segment
    FROM reconciled a
    JOIN customers c
        ON a.customer_id = c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v,
        age,
        segment,
        SUM(age) OVER (PARTITION BY segment ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS age_sum_2h,
        MIN(age) OVER (PARTITION BY segment) AS segment_min_age,
        AVG(age) OVER (PARTITION BY k) AS k_avg_age
    FROM with_dims
)
SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    k,
    v,
    segment,
    segment_min_age,
    k_avg_age,
    CASE WHEN segment RLIKE '^(SMB|Corporate)$' THEN 'CORE' WHEN segment RLIKE '^(Consumer)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS segment_zone
FROM metrics
ORDER BY segment_zone NULLS FIRST;
