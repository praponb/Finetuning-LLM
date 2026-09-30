WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date
    FROM policies
    WHERE product IS NOT NULL
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.end_date_month,
        s.days_since_start_date,
        p.holder_name
    FROM recent_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        holder_id,
        ROUND(AVG(days_since_start_date), 2) AS avg_days_since_start_date,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids
    FROM screened
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), holder_id
)
SELECT
    start_date_ym,
    holder_id,
    avg_days_since_start_date,
    distinct_holder_ids,
    COALESCE(distinct_holder_ids, 0) AS distinct_holder_ids_filled,
    COALESCE(distinct_holder_ids, 0) AS distinct_holder_ids_filled_2
FROM per_group
ORDER BY avg_days_since_start_date DESC
