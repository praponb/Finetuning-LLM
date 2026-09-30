WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        coupon_codes,
        attr_key,
        value,
        SIZE(coupon_codes) AS coupon_codes_count,
        COALESCE(attr_key, 'OTHER') AS attr_key_filled
    FROM lines
    WHERE created_at > DATE '2025-03-02'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY coupon_codes_count NULLS FIRST) AS coupon_codes_count_percent_rank,
        AVG(coupon_codes_count) OVER (PARTITION BY customer_id, attr_key) AS customer_id_avg_coupon_codes_count
    FROM base
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.signup_date,
        p.credit_limit,
        p.email
    FROM scored e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        attr_key,
        SUM(CASE WHEN attr_key = 'gift' THEN coupon_codes_count_percent_rank ELSE 0 END) AS gift_coupon_codes_count_percent_rank,
        SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
        PERCENTILE_APPROX(coupon_codes_count_percent_rank, 0.9) AS coupon_codes_count_percent_rank_p90
    FROM with_dims
    GROUP BY attr_key
)
SELECT
    attr_key,
    gift_coupon_codes_count_percent_rank,
    num_attr_keys,
    coupon_codes_count_percent_rank_p90
FROM grouped;
