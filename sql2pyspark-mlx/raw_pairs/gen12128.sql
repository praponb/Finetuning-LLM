WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY status, plan ORDER BY monthly_fee DESC NULLS FIRST) AS monthly_fee_cume_dist,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, status), 4) AS monthly_fee_share,
        FIRST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY YEAR(start_date), status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee
    FROM subscriptions
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY plan ORDER BY monthly_fee_cume_dist DESC NULLS LAST) AS row_num
        FROM with_windows
    ) t
    WHERE row_num = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM ranked src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date >= sub.start_date
       AND src.end_date < sub.end_date + INTERVAL 7 DAYS
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        monthly_fee_cume_dist,
        monthly_fee_share,
        first_known_monthly_fee,
        row_num,
        subscription_id,
        subscription_plan,
        subscription_monthly_fee,
        CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END AS is_paused,
        COALESCE(subscription_plan, 'DIRECT') AS subscription_plan_filled
    FROM attributed
    WHERE end_date >= DATE '2023-10-09'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.monthly_fee,
        a.status,
        a.monthly_fee_cume_dist,
        a.monthly_fee_share,
        a.first_known_monthly_fee,
        a.subscription_id,
        a.subscription_plan,
        a.is_paused,
        a.subscription_plan_filled,
        b.credit_limit,
        b.country
    FROM filtered a
    INNER JOIN customers b
        ON a.customer_id = b.id
)
SELECT
    id,
    monthly_fee,
    status,
    monthly_fee_cume_dist,
    monthly_fee_share,
    first_known_monthly_fee,
    subscription_id,
    subscription_plan,
    is_paused
FROM with_dims
