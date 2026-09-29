WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        entry.product_id,
        entry.qty,
        entry.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS entry
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.created_at,
        e.coupon_codes,
        e.promo,
        b.credit_limit
    FROM flattened e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_part AS credit_limit_percent_rank,
        SUM(credit_limit) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS credit_limit_sum_15m,
        MIN(credit_limit) OVER w_running AS customer_id_min_credit_limit
    FROM joined
    WHERE customer_id IN (3, 12, 13, 31)
    WINDOW w_part AS (PARTITION BY promo ORDER BY credit_limit DESC), w_running AS (PARTITION BY customer_id)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.name,
        b.credit_limit AS customer_credit_limit
    FROM metrics e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN credit_limit_sum_15m >= 76220 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_sum_15m_tier,
        DATE_TRUNC('DAY', created_at) AS activity_day,
        customer_id,
        SIZE(COLLECT_SET(promo)) AS num_promos,
        SIZE(COLLECT_SET(promo)) AS num_promos_2,
        MAX(created_at) AS last_created_at,
        PERCENTILE_APPROX(credit_limit_sum_15m, 0.5) AS credit_limit_sum_15m_p50
    FROM enriched
    GROUP BY credit_limit_sum_15m_tier, activity_day, customer_id
    HAVING credit_limit_sum_15m_p50 > 0
)
SELECT
    credit_limit_sum_15m_tier,
    activity_day,
    customer_id,
    last_created_at
FROM grouped
ORDER BY activity_day DESC, last_created_at;
