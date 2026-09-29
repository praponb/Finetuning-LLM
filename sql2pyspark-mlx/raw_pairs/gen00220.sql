WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM subscriptions s
    JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date >= sub.start_date
       AND s.start_date < sub.end_date + INTERVAL '14' DAY
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(subscription_monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_subscription_monthly_fee,
        MAX(subscription_monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_subscription_monthly_fee,
        MIN(subscription_monthly_fee) OVER (PARTITION BY plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_subscription_monthly_fee
    FROM attributed
    WHERE monthly_fee BETWEEN 80 AND 190
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY plan ORDER BY last3_max_subscription_monthly_fee DESC) = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.age,
        p.credit_limit
    FROM ranked e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
base AS (
    -- Keep recent, valid rows
    SELECT *, last3_max_subscription_monthly_fee - last3_count_subscription_monthly_fee AS last3_max_subscription_monthly_fee_minus_last3_count_subscription_monthly_fee
    FROM joined
    WHERE subscription_plan IS NOT DISTINCT FROM 'enterprise'
      AND subscription_plan IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT status, id
    FROM base
    INTERSECT
    SELECT status, id
    FROM subscriptions
    WHERE YEAR(end_date) >= 2024
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
)
SELECT
    plan,
    status,
    subscription_id,
    subscription_plan,
    subscription_monthly_fee,
    last3_count_subscription_monthly_fee,
    last3_max_subscription_monthly_fee,
    age,
    last3_max_subscription_monthly_fee_minus_last3_count_subscription_monthly_fee
FROM verified;
