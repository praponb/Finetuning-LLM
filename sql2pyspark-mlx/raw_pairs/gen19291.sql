WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM subscriptions a
    JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.end_date >= sub.start_date
       AND a.end_date < sub.end_date + INTERVAL '14' DAY
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee_2
    FROM matched e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.start_date >= sub.start_date
       AND e.start_date < sub.end_date + INTERVAL '30' DAY
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, p.name
    FROM with_window src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(subscription_id_2 * 0.9, 2) AS adjusted_subscription_id_2,
        CAST(subscription_monthly_fee_2 AS INT) AS subscription_monthly_fee_2_int,
        COALESCE(subscription_plan, 'OTHER') AS subscription_plan_clean
    FROM enriched
    WHERE subscription_id <= 8
),
remaining_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM base
    EXCEPT ALL
    SELECT plan
    FROM subscriptions
    WHERE monthly_fee >= 60
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.plan = k.plan
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        plan,
        subscription_monthly_fee_2,
        adjusted_subscription_id_2,
        subscription_monthly_fee
    FROM verified
) src
UNPIVOT (
    reading FOR metric_name IN (subscription_monthly_fee_2 AS SUBSCRIPTION_MONTHLY_FEE_2, adjusted_subscription_id_2, subscription_monthly_fee)
);
