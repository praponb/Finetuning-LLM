WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(status, ' ', plan) AS status_plan,
        CASE WHEN status IN ('PAUSED', 'EXPIRED') THEN 'Priority' ELSE 'Other' END AS status_group
    FROM subscriptions
    WHERE MONTH(end_date) IN (3, 7, 9)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        monthly_fee,
        status_plan,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share,
        AVG(monthly_fee) OVER w_grp AS monthly_fee_avg_5
    FROM base
    WINDOW w_grp AS (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(monthly_fee) OVER w_part AS last4_sum_monthly_fee,
        AVG(monthly_fee) OVER w_part AS last4_avg_monthly_fee,
        COUNT(monthly_fee) OVER w_part AS last4_count_monthly_fee
    FROM deduped
    WINDOW w_part AS (PARTITION BY plan ORDER BY monthly_fee_avg_5, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN last4_avg_monthly_fee >= 230 THEN 'HIGH' ELSE 'LOW' END AS last4_avg_monthly_fee_tier,
        plan,
        COUNT(CASE WHEN plan = 'standard' THEN 1 END) AS standard_count,
        ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN plan = 'premium' THEN 1 END), 0), 2) AS monthly_fee_per_premium,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count,
        COUNT_IF(plan = 'basic') AS basic_events
    FROM enriched
    GROUP BY CUBE (CASE WHEN last4_avg_monthly_fee >= 230 THEN 'HIGH' ELSE 'LOW' END, plan)
)
SELECT
    plan,
    standard_count,
    distinct_ids,
    enterprise_count,
    basic_events
FROM grouped
ORDER BY distinct_ids DESC;
