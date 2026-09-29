WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
    FROM policies
    WHERE product IS NOT NULL
),
latest_per_key AS (
    -- Keep the latest row per holder_id, status
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        start_date_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM base_policys
    ) ranked
    WHERE recency_rank = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        premium,
        start_date,
        end_date,
        status,
        start_date_label,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label_2
    FROM latest_per_key
    WHERE start_date_label IS NOT NULL
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        COUNT(DISTINCT status) AS distinct_status,
        MIN(end_date) AS first_end_date,
        MIN(start_date) AS first_start_date,
        COUNT(*) AS cnt,
        ROUND(AVG(premium), 2) AS avg_premium
    FROM labeled
    GROUP BY DATE_TRUNC('MONTH', end_date)
    HAVING SUM(premium) > 1051.9
)
SELECT
    end_date_month,
    distinct_status,
    cnt,
    avg_premium
FROM grouped
WHERE cnt >= 3
ORDER BY end_date_month, avg_premium DESC NULLS FIRST
LIMIT 10
