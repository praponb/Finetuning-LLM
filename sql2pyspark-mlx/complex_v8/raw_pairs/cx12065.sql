WITH valid_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        is_international,
        DATEDIFF(DATE '2026-01-01', enrolled_on) AS days_since_enrolled_on,
        SUBSTRING(TRIM(major), 1, 3) AS major_prefix,
        ROUND(gpa * 0.9, 2) AS gpa_adj
    FROM students
    WHERE major NOT IN ('Art', 'Math')
      AND gpa > 2.0
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        is_international,
        days_since_enrolled_on,
        gpa_adj,
        ROUND(AVG(days_since_enrolled_on) OVER (PARTITION BY DATE_TRUNC('MONTH', enrolled_on), is_international ORDER BY days_since_enrolled_on ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS days_since_enrolled_on_avg_4,
        NTH_VALUE(days_since_enrolled_on, 2) OVER (ORDER BY gpa_adj ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_enrolled_on
    FROM valid_students
),
latest_per_key AS (
    -- Keep the latest row per cohort
    SELECT
        id,
        student_name,
        cohort,
        enrolled_on,
        is_international,
        days_since_enrolled_on,
        gpa_adj,
        days_since_enrolled_on_avg_4,
        second_days_since_enrolled_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY enrolled_on DESC, id DESC) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    student_name,
    cohort,
    enrolled_on,
    days_since_enrolled_on,
    gpa_adj,
    days_since_enrolled_on_avg_4,
    second_days_since_enrolled_on,
    CASE WHEN days_since_enrolled_on >= 560 THEN 'large' WHEN days_since_enrolled_on >= 210 THEN 'medium' ELSE 'small' END AS days_since_enrolled_on_tier
FROM latest_per_key
ORDER BY enrolled_on DESC, second_days_since_enrolled_on DESC
