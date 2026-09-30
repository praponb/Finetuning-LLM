WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        ROUND(monthly_fee, 1) AS monthly_fee_rounded
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(monthly_fee_rounded, 1) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS next_monthly_fee_rounded,
        FIRST_VALUE(monthly_fee_rounded) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY monthly_fee_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee_rounded
    FROM deduped
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM metrics a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date < sub.end_date + INTERVAL 14 DAYS
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('MONTH', start_date) AS activity_month,
        APPROX_PERCENTILE(monthly_fee, 0.9) AS monthly_fee_p90,
        COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_rows,
        SIZE(COLLECT_SET(status)) AS num_status,
        SUM(CASE WHEN status = 'CANCELLED' THEN first_known_monthly_fee_rounded ELSE 0 END) AS cancelled_first_known_monthly_fee_rounded,
        SIZE(COLLECT_SET(status)) AS num_status_2
    FROM attributed
    GROUP BY DATE_TRUNC('MONTH', start_date)
)
SELECT
    activity_month,
    monthly_fee_p90,
    expired_rows,
    num_status,
    cancelled_first_known_monthly_fee_rounded,
    num_status_2
FROM stats
ORDER BY expired_rows;
