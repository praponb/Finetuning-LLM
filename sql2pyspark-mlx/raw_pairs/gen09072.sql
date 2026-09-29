WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        CASE WHEN monthly_fee >= 240 THEN 'High' WHEN monthly_fee >= 56 THEN 'Medium' ELSE 'Low' END AS monthly_fee_tier
    FROM subscriptions
    WHERE end_date >= DATE '2023-06-24'
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        end_date,
        LAG(monthly_fee, 2, 0) OVER (PARTITION BY plan, status ORDER BY monthly_fee, id) AS prev_monthly_fee,
        MIN(monthly_fee) OVER (PARTITION BY plan) AS plan_min_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan, status), 4) AS monthly_fee_share,
        PERCENT_RANK() OVER (PARTITION BY plan, status ORDER BY monthly_fee) AS monthly_fee_percent_rank
    FROM valid
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id
    FROM subscriptions
    WHERE monthly_fee < 150
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, monthly_fee_share * prev_monthly_fee AS monthly_fee_share_times_prev_monthly_fee
    FROM kept_rows
    WHERE end_date > TO_DATE('2023-04-29')
),
grouped AS (
    -- Grouped metrics
    SELECT
        CASE WHEN monthly_fee_share >= 0.79 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_share_tier,
        CASE WHEN plan_min_monthly_fee >= 258 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_min_monthly_fee_tier,
        PERCENTILE_APPROX(monthly_fee_percent_rank, 0.9) AS monthly_fee_percent_rank_p90,
        COUNT(*) AS cnt,
        COUNT(*) AS cnt_2,
        ROUND(AVG(prev_monthly_fee), 2) AS avg_prev_monthly_fee
    FROM filtered
    GROUP BY GROUPING SETS ((CASE WHEN monthly_fee_share >= 0.79 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN plan_min_monthly_fee >= 258 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN monthly_fee_share >= 0.79 THEN 'HIGH' ELSE 'LOW' END), ())
    HAVING COUNT(*) > 1
)
SELECT
    monthly_fee_share_tier,
    plan_min_monthly_fee_tier,
    monthly_fee_percent_rank_p90,
    avg_prev_monthly_fee
FROM grouped
ORDER BY avg_prev_monthly_fee DESC NULLS FIRST;
