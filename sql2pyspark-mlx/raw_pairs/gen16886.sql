WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(monthly_fee) OVER w_hist AS running_max_monthly_fee,
        AVG(monthly_fee) OVER w_hist AS running_avg_monthly_fee,
        COUNT(monthly_fee) OVER w_hist AS running_count_monthly_fee,
        MIN(monthly_fee) OVER w_hist AS running_min_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee, id) AS seq_no
    FROM subscriptions
    WHERE plan = 'enterprise'
    WINDOW w_hist AS (PARTITION BY plan, status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        plan,
        start_date,
        monthly_fee,
        status,
        running_avg_monthly_fee
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        start_date,
        monthly_fee,
        status,
        running_avg_monthly_fee,
        latest_rank,
        RANK() OVER (ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC) AS monthly_fee_percent_rank,
        SUM(running_avg_monthly_fee) OVER (PARTITION BY MONTH(start_date), status ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_avg_monthly_fee
    FROM deduped
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(status) AS status_lower
    FROM with_windows
    WHERE start_date > DATE '2023-09-05'
      AND YEAR(start_date) >= 2024
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM filtered
    EXCEPT DISTINCT
    SELECT status, id
    FROM subscriptions
    WHERE end_date <= '2025-08-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', start_date) AS year_start,
    status,
    APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM kept_rows
GROUP BY CUBE (DATE_TRUNC('YEAR', start_date), status)
HAVING distinct_ids > 1
ORDER BY status NULLS LAST;
