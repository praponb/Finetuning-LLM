WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
    WHERE premium > 1132.1
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(start_date_label, '#', CAST(holder_id AS STRING)) AS start_date_label_ref
    FROM cleaned
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(DISTINCT status) AS distinct_status,
        ROUND(AVG(premium), 2) AS avg_premium,
        ROUND(100 * COUNT(CASE WHEN status = 'LAPSED' THEN 1 END) / COUNT(*), 1) AS pct_status_lapsed
    FROM annotated
    GROUP BY 1
    HAVING AVG(premium) > 2033.8
)
SELECT
    start_date_month,
    cnt,
    pct_status_lapsed,
    cnt - pct_status_lapsed AS cnt_minus_pct_status_lapsed,
    ABS(avg_premium - 3080.0) AS avg_premium_distance
FROM stats
WHERE avg_premium BETWEEN 440.0 AND 3080.0
ORDER BY cnt NULLS LAST, cnt_minus_pct_status_lapsed, start_date_month
LIMIT 20
