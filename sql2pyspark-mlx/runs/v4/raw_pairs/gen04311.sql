WITH base AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        CASE WHEN monthly_fee >= 268 THEN 'High' WHEN monthly_fee >= 35 THEN 'Medium' ELSE 'Low' END AS monthly_fee_tier
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-08-02')
),
joined AS (
    SELECT
        src.*,
        c.name,
        c.credit_limit,
        c.segment
    FROM base src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    SELECT
        *,
        SUM(credit_limit) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        DENSE_RANK() OVER (PARTITION BY segment, plan ORDER BY credit_limit DESC) AS credit_limit_dense_rank
    FROM joined
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY end_date DESC, id) AS rank_in_group
        FROM metrics
    ) ranked
    WHERE rank_in_group = 1
),
metrics_subscriptions AS (
    SELECT
        *,
        LAG(running_credit_limit, 1) OVER w_recent AS prev_running_credit_limit,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY segment ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_2,
        LAST_VALUE(running_credit_limit) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY last_known_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_credit_limit
    FROM ranked
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY running_credit_limit, id)
),
matched AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM metrics_subscriptions a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date <= DATE_ADD(sub.end_date, 7)
)
SELECT
    *,
    AVG(last_known_monthly_fee) OVER (PARTITION BY segment ORDER BY rank_in_group, id, subscription_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last_known_monthly_fee_avg_6_prior,
    AVG(prev_running_credit_limit) OVER (ORDER BY running_credit_limit, id, subscription_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS prev_running_credit_limit_avg_6,
    AVG(prev_running_credit_limit) OVER (ORDER BY rank_in_group, id, subscription_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_running_credit_limit_avg_2_prior
FROM matched;
