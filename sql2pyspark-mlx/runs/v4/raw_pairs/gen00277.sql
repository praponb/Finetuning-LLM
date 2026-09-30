WITH base AS (
    SELECT
        *,
        CASE WHEN browser = 'Safari' THEN 1 ELSE 0 END AS is_safari,
        CONCAT(browser, ' - ', device) AS browser_device
    FROM sessions
    WHERE started_at >= DATE '2023-10-19'
      AND browser IS NOT NULL
),
attributed AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base a
    JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 7 DAYS
),
valid AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        is_safari,
        browser_device,
        subscription_id,
        plan,
        monthly_fee,
        ROUND(monthly_fee, 1) AS monthly_fee_rounded,
        COALESCE(plan, 'NONE') AS plan_clean
    FROM attributed
    WHERE started_at > TIMESTAMP '2023-11-04 09:00:00'
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        is_safari,
        browser_device,
        subscription_id,
        plan,
        monthly_fee,
        monthly_fee_rounded,
        plan_clean,
        AVG(monthly_fee_rounded) OVER (PARTITION BY customer_id) AS customer_id_avg_monthly_fee_rounded,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY subscription_id, id, subscription_id) AS subscription_id_bucket,
        SUM(is_safari) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_safari_sum_7d,
        AVG(subscription_id) OVER (ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS subscription_id_avg_3_prior,
        FIRST_VALUE(plan, TRUE) OVER (PARTITION BY plan ORDER BY is_safari, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_plan
    FROM valid
)
SELECT *
FROM (
    SELECT
        id,
        subscription_id,
        device,
        monthly_fee,
        monthly_fee_rounded
    FROM enriched
) src
UNPIVOT (
    metric_value FOR measure IN (monthly_fee AS MONTHLY_FEE_VAL, monthly_fee_rounded)
)
