WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id ASC) monthly_fee_bucket,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_monthly_fee,
        DENSE_RANK() OVER (ORDER BY monthly_fee ASC) monthly_fee_dense_rank,
        FIRST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY MONTH(end_date), customer_id ORDER BY monthly_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee
    FROM subscriptions
),
enriched_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status,
        monthly_fee_dense_rank,
        COUNT(last_known_monthly_fee) OVER w_ordered running_count_last_known_monthly_fee,
        MIN(last_known_monthly_fee) OVER w_ordered running_min_last_known_monthly_fee,
        MAX(last_known_monthly_fee) OVER w_ordered running_max_last_known_monthly_fee
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY status ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_dense_rank ASC) AS rank_in_group
        FROM enriched_subscriptions
    ) ranked
    WHERE rank_in_group = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM ranked a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.end_date BETWEEN sub.start_date AND sub.end_date
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee
    FROM attributed e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.end_date >= sub.start_date
       AND e.end_date < sub.end_date + INTERVAL 30 DAYS
)
-- Join a dimension and aggregate
SELECT
    dim.country,
    a.subscription_plan,
    COUNT(*) AS cnt,
    ROUND(AVG(a.running_min_last_known_monthly_fee), 2) AS avg_running_min_last_known_monthly_fee,
    ROUND(SUM(a.running_count_last_known_monthly_fee), 2) AS total_running_count_last_known_monthly_fee,
    GROUPING_ID() AS level_id
FROM matched a
INNER JOIN customers dim ON a.customer_id = dim.id
GROUP BY GROUPING SETS ((dim.country, a.subscription_plan), (dim.country), ())
ORDER BY avg_running_min_last_known_monthly_fee ASC
