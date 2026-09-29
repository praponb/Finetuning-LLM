WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(status, ' - ', plan) AS status_plan
    FROM subscriptions
    WHERE start_date > DATE '2023-09-09'
      AND start_date >= '2024-01-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        monthly_fee,
        LAG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id) AS prev_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) AS row_seq
    FROM deduped
    WHERE status IN ('ACTIVE', 'CANCELLED', 'EXPIRED')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT
    SELECT id
    FROM subscriptions
    WHERE end_date <= '2025-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.id = k.id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        prev_monthly_fee * row_seq AS prev_monthly_fee_times_row_seq,
        row_seq + prev_monthly_fee AS row_seq_plus_prev_monthly_fee
    FROM verified
    WHERE prev_monthly_fee BETWEEN 80 AND 90
)
-- Grouped metrics
SELECT
    CASE WHEN prev_monthly_fee >= 189 THEN 'HIGH' ELSE 'LOW' END AS prev_monthly_fee_tier,
    CASE WHEN row_seq_plus_prev_monthly_fee >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_plus_prev_monthly_fee_tier,
    PERCENTILE_APPROX(row_seq_plus_prev_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS row_seq_plus_prev_monthly_fee_quartiles,
    SUM(monthly_fee) AS total_monthly_fee,
    GROUPING(CASE WHEN prev_monthly_fee >= 189 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN prev_monthly_fee >= 189 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN row_seq_plus_prev_monthly_fee >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN prev_monthly_fee >= 189 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY prev_monthly_fee_tier DESC NULLS FIRST;
