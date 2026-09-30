WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        SUBSTRING(device, 1, 4) device_prefix,
        COALESCE(browser, 'NONE') browser_filled
    FROM sessions
    WHERE browser IS NOT NULL
      AND browser IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at <= DATE_ADD(sub.end_date, 10)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_window src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at BETWEEN sub.start_date AND sub.end_date
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        browser,
        browser_filled,
        subscription_id,
        plan,
        monthly_fee,
        subscription_id_2,
        subscription_plan,
        subscription_monthly_fee,
        monthly_fee * subscription_monthly_fee monthly_fee_times_subscription_monthly_fee
    FROM attributed
    WHERE browser IS NOT NULL
      AND started_at >= '2023-05-01'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        browser,
        subscription_plan,
        subscription_id_2
    FROM base
) src
PIVOT (
    SUM(subscription_id_2)
    FOR subscription_plan IN ('premium' AS premium, 'enterprise' AS enterprise)
);
