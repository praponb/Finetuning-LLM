WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS key, v
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.email,
        c.signup_date
    FROM flattened s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_dims e
    INNER JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.signup_date >= sub.start_date
       AND e.signup_date < sub.end_date + INTERVAL '30' DAY
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY monthly_fee DESC, id, key, subscription_id) AS dedup_rank
    FROM with_window
    QUALIFY dedup_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, subscription_id * dedup_rank AS subscription_id_times_dedup_rank
    FROM deduped
    WHERE key IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(subscription_id) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee, id, key, subscription_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_5_prior,
        NTILE(10) OVER (PARTITION BY plan, key ORDER BY subscription_id DESC, id, key, subscription_id) AS subscription_id_bucket,
        COUNT(monthly_fee) OVER (PARTITION BY plan) AS plan_count_monthly_fee
    FROM filtered
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        key,
        plan,
        plan_count_monthly_fee
    FROM metrics
) src
PIVOT (
    SUM(plan_count_monthly_fee) AS total,
    COUNT(1) AS cnt
    FOR plan IN ('standard', 'enterprise', 'basic')
)
