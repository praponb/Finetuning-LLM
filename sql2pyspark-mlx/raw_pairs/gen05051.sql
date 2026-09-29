WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        start_date,
        end_date,
        ROUND(monthly_fee * 0.85, 2) AS adjusted_monthly_fee
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM valid
    EXCEPT
    SELECT id, plan
    FROM subscriptions
    WHERE customer_id IN (13, 14, 24, 37)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN adjusted_monthly_fee >= 75 THEN 'High' WHEN adjusted_monthly_fee >= 32 THEN 'Medium' ELSE 'Low' END AS adjusted_monthly_fee_band
    FROM reconciled
    WHERE start_date >= DATE '2023-05-08'
),
cleaned_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        start_date,
        adjusted_monthly_fee,
        adjusted_monthly_fee_band,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM cleaned
    WHERE start_date >= TO_DATE('2023-09-01')
      AND adjusted_monthly_fee <= 61
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        start_date,
        adjusted_monthly_fee,
        end_date_label,
        DATE_TRUNC('month', start_date) AS start_date_period,
        DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM cleaned_subscriptions
    WHERE adjusted_monthly_fee < 21
)
-- Multi-level aggregation
SELECT
    CASE WHEN adjusted_monthly_fee >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_monthly_fee_tier,
    DATE_TRUNC('YEAR', start_date_period) AS year_start,
    CASE WHEN days_since_start_date >= 85 THEN 'LARGE' ELSE 'SMALL' END AS days_since_start_date_tier,
    MAX(start_date_period) AS last_seen,
    MIN(start_date_period) AS first_start_date_period,
    (UNIX_TIMESTAMP(MAX(start_date_period)) - UNIX_TIMESTAMP(MIN(start_date_period))) / 60 AS duration_min,
    ROUND(AVG(days_since_start_date), 2) AS avg_days_since_start_date,
    ROUND(AVG(days_since_start_date), 2) AS avg_days_since_start_date_2,
    (UNIX_TIMESTAMP(MAX(start_date_period)) - UNIX_TIMESTAMP(MIN(start_date_period))) / 60 AS duration_min_2
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN adjusted_monthly_fee >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', start_date_period), CASE WHEN days_since_start_date >= 85 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN adjusted_monthly_fee >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', start_date_period)), (CASE WHEN adjusted_monthly_fee >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END))
HAVING avg_days_since_start_date_2 > 0
ORDER BY avg_days_since_start_date NULLS LAST;
