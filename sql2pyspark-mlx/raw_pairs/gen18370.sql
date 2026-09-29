WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank,
        SUM(monthly_fee) OVER (ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
    FROM subscriptions
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_windows src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date BETWEEN sub.start_date AND sub.end_date
),
with_windows_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(subscription_monthly_fee) OVER (PARTITION BY subscription_plan ORDER BY monthly_fee_rank, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_subscription_monthly_fee,
        AVG(subscription_monthly_fee) OVER (PARTITION BY subscription_plan ORDER BY monthly_fee_rank, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_subscription_monthly_fee,
        COUNT(subscription_monthly_fee) OVER (PARTITION BY subscription_plan ORDER BY monthly_fee_rank, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_subscription_monthly_fee,
        MAX(subscription_monthly_fee) OVER (PARTITION BY subscription_plan ORDER BY monthly_fee_rank, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_subscription_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY subscription_plan ORDER BY start_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM with_window
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        status,
        monthly_fee_rank,
        running_monthly_fee,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        last7_min_subscription_monthly_fee,
        last7_avg_subscription_monthly_fee,
        SUM(running_monthly_fee) OVER w_running AS last3_sum_running_monthly_fee,
        COUNT(running_monthly_fee) OVER w_running AS last3_count_running_monthly_fee,
        MIN(running_monthly_fee) OVER w_running AS last3_min_running_monthly_fee
    FROM with_windows_subscriptions
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY running_monthly_fee, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(last7_avg_subscription_monthly_fee AS INT) AS last7_avg_subscription_monthly_fee_int,
        COALESCE(subscription_plan, 'NONE') AS subscription_plan_clean
    FROM scored
    WHERE subscription_plan IN ('basic', 'premium')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(last3_sum_running_monthly_fee) OVER (PARTITION BY plan, status ORDER BY last7_min_subscription_monthly_fee, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_last3_sum_running_monthly_fee,
        SUM(last3_sum_running_monthly_fee) OVER (PARTITION BY plan, status ORDER BY last7_min_subscription_monthly_fee, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_last3_sum_running_monthly_fee,
        AVG(last3_sum_running_monthly_fee) OVER (PARTITION BY plan, status ORDER BY last7_min_subscription_monthly_fee, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_last3_sum_running_monthly_fee,
        AVG(last3_count_running_monthly_fee) OVER (PARTITION BY status, plan) AS status_avg_last3_count_running_monthly_fee
    FROM base
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        monthly_fee_rank,
        running_monthly_fee,
        subscription_id,
        subscription_monthly_fee,
        last7_avg_subscription_monthly_fee_int,
        last7_sum_last3_sum_running_monthly_fee,
        last7_avg_last3_sum_running_monthly_fee
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY last7_max_last3_sum_running_monthly_fee DESC NULLS FIRST, id, subscription_id) = 1
)
-- Multi-level aggregation
SELECT
    subscription_id,
    PERCENTILE_APPROX(subscription_id, ARRAY(0.25, 0.5, 0.75)) AS subscription_id_quartiles,
    PERCENTILE_APPROX(last7_avg_last3_sum_running_monthly_fee, 0.95) AS last7_avg_last3_sum_running_monthly_fee_p95
FROM ranked
GROUP BY subscription_id
