WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        monthly_fee,
        status,
        CASE WHEN plan IN ('premium', 'enterprise') THEN 'Core' ELSE 'Other' END AS plan_group,
        FLOOR(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-04-24')
      AND customer_id = 36
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY monthly_fee_whole DESC) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        monthly_fee,
        plan_group,
        monthly_fee_whole,
        dedup_rank,
        SUM(monthly_fee_whole) OVER w_part AS running_sum_monthly_fee_whole,
        MIN(monthly_fee_whole) OVER w_part AS running_min_monthly_fee_whole,
        AVG(monthly_fee_whole) OVER w_part AS running_avg_monthly_fee_whole,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER w_part AS last_known_monthly_fee
    FROM deduped
    WINDOW w_part AS (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan_group,
        monthly_fee_whole,
        running_sum_monthly_fee_whole,
        last_known_monthly_fee,
        LAG(running_avg_monthly_fee_whole) OVER w_grp AS prev_running_avg_monthly_fee_whole,
        LEAD(running_avg_monthly_fee_whole) OVER w_grp AS next_running_avg_monthly_fee_whole,
        LEAD(last_known_monthly_fee) OVER w_grp AS next_last_known_monthly_fee,
        LAG(last_known_monthly_fee) OVER w_grp AS prev_last_known_monthly_fee,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM metrics
    WINDOW w_grp AS (ORDER BY monthly_fee_whole, id)
)
-- Grouped metrics
SELECT
    CASE WHEN prev_last_known_monthly_fee >= 263 THEN 'LARGE' ELSE 'SMALL' END AS prev_last_known_monthly_fee_tier,
    CASE WHEN last_known_monthly_fee >= 231 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_monthly_fee_tier,
    CASE WHEN next_last_known_monthly_fee >= 261 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_last_known_monthly_fee_tier,
    ROUND(AVG(monthly_fee_whole), 2) AS avg_monthly_fee_whole,
    SUM(last_known_monthly_fee) AS total_last_known_monthly_fee,
    RANK() OVER (PARTITION BY CASE WHEN prev_last_known_monthly_fee >= 263 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(AVG(monthly_fee_whole), 2) DESC) AS avg_monthly_fee_whole_rank
FROM with_windows
GROUP BY 1, 2, 3
