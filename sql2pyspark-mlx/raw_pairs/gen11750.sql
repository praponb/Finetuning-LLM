WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(monthly_fee) OVER w_grp AS next_monthly_fee,
        ROW_NUMBER() OVER w_grp AS row_seq,
        SUM(monthly_fee) OVER w_part AS running_monthly_fee
    FROM subscriptions
    WHERE monthly_fee <= 120
    WINDOW w_grp AS (PARTITION BY plan, status ORDER BY end_date, id), w_part AS (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        next_monthly_fee,
        row_seq
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY plan, customer_id ORDER BY end_date DESC) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(row_seq, 1) AS row_seq_rounded
    FROM deduped
    WHERE start_date >= DATE '2023-04-10'
      AND customer_id IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM filtered e
    JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date BETWEEN sub.start_date AND sub.end_date
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(monthly_fee) OVER w_hist AS last3_count_monthly_fee,
        MIN(monthly_fee) OVER w_hist AS last3_min_monthly_fee,
        SUM(monthly_fee) OVER w_hist AS last3_sum_monthly_fee
    FROM matched
    WINDOW w_hist AS (PARTITION BY plan ORDER BY row_seq, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        row_seq,
        subscription_id,
        subscription_plan,
        last3_sum_monthly_fee,
        SUM(subscription_id) OVER w_running AS running_sum_subscription_id,
        MAX(subscription_id) OVER w_running AS running_max_subscription_id,
        COUNT(subscription_id) OVER w_running AS running_count_subscription_id
    FROM metrics
    WINDOW w_running AS (PARTITION BY status ORDER BY row_seq_rounded, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY subscription_plan ORDER BY row_seq, id, subscription_id) AS row_num
        FROM enriched_subscriptions
    ) ranked
    WHERE row_num = 1
)
-- Aggregate with conditional metrics
SELECT
    plan,
    customer_id,
    COUNT(*) AS num_rows,
    AVG(running_max_subscription_id) FILTER (WHERE subscription_plan = 'premium') AS premium_avg_running_max_subscription_id,
    COUNT_IF(subscription_plan = 'standard') AS standard_events,
    MIN(running_sum_subscription_id) AS min_running_sum_subscription_id,
    SUM(CASE WHEN subscription_plan = 'premium' THEN running_sum_subscription_id ELSE 0 END) AS premium_running_sum_subscription_id,
    PERCENTILE_APPROX(running_max_subscription_id, ARRAY(0.25, 0.5, 0.75)) AS running_max_subscription_id_quartiles
FROM latest
GROUP BY plan, customer_id;
