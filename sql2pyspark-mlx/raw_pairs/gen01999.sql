WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date
    FROM subscriptions
    QUALIFY RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee) = 1
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM deduped s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date BETWEEN sub.start_date AND sub.end_date
),
enriched AS (
    -- Attach customers attributes
    SELECT s.*, d.age
    FROM matched s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        end_date,
        subscription_id,
        subscription_plan,
        SUM(age) OVER (PARTITION BY plan, subscription_plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_age,
        COUNT(age) OVER (PARTITION BY plan, subscription_plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age,
        AVG(age) OVER (PARTITION BY plan, subscription_plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_age
    FROM enriched
    WHERE age = 41
)
-- Join a dimension and aggregate
SELECT
    p.country,
    s.subscription_plan,
    COUNT(*) AS num_rows,
    AVG(s.running_sum_age) AS avg_running_sum_age,
    MAX(s.subscription_id) AS max_subscription_id
FROM metrics s
INNER JOIN customers p ON s.customer_id = p.id
GROUP BY GROUPING SETS ((p.country, s.subscription_plan), (p.country), ())
ORDER BY max_subscription_id DESC NULLS FIRST
