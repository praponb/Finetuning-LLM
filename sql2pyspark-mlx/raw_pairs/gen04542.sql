WITH valid AS (
    SELECT
        id,
        customer_id,
        monthly_fee,
        LENGTH(plan) plan_length
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
valid_subscriptions AS (
    SELECT
        *,
        LEAST(monthly_fee, plan_length) least_monthly_fee_plan_length,
        ROUND(plan_length * 1.1, 2) plan_length_scaled
    FROM valid
    WHERE plan_length != 24
),
metrics AS (
    SELECT
        id,
        customer_id,
        plan_length_scaled,
        SUM(plan_length) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_plan_length,
        AVG(plan_length) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_plan_length,
        MAX(plan_length) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_plan_length,
        AVG(plan_length) OVER (PARTITION BY customer_id ORDER BY least_monthly_fee_plan_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) plan_length_avg_6
    FROM valid_subscriptions
    WHERE plan_length_scaled > 54
),
filtered AS (
    SELECT
        id,
        customer_id,
        plan_length_scaled,
        running_sum_plan_length,
        plan_length_avg_6,
        LEAST(plan_length_scaled, running_max_plan_length) least_plan_length_scaled_running_max_plan_length
    FROM metrics
    WHERE customer_id IS NOT NULL
),
enriched AS (
    SELECT e.*, b.credit_limit
    FROM filtered e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
matched_keys AS (
    SELECT id, customer_id
    FROM enriched
    EXCEPT ALL
    SELECT id, customer_id
    FROM subscriptions
    WHERE status = 'EXPIRED'
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
)
SELECT
    id,
    plan_length_scaled,
    running_sum_plan_length,
    credit_limit
FROM verified
ORDER BY running_sum_plan_length NULLS FIRST;
