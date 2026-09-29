WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', plan, status) AS plan_status_key
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-10-23')
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        plan_status_key,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee DESC) AS monthly_fee_percent_rank,
        DENSE_RANK() OVER (ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_dense_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status, plan), 4) AS monthly_fee_share,
        LAG(monthly_fee, 1, 0) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS prev_monthly_fee
    FROM base
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.signup_date,
        c.segment
    FROM metrics a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM joined a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date < sub.end_date + INTERVAL 7 DAYS
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    subscription_id,
    customer_id,
    measure,
    value
FROM (
    SELECT
        id,
        subscription_id,
        customer_id,
        monthly_fee,
        monthly_fee_percent_rank,
        prev_monthly_fee
    FROM with_window
) src
UNPIVOT (
    value FOR measure IN (monthly_fee AS MONTHLY_FEE_VAL, monthly_fee_percent_rank AS MONTHLY_FEE_PERCENT_RANK_VAL, prev_monthly_fee AS PREV_MONTHLY_FEE)
) long_rows
