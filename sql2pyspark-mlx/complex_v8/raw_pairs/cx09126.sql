WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DATE_ADD(end_date, 30) AS end_date_plus_30d,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE start_date >= DATE '2024-10-22'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.status_slug,
        s.end_date_plus_30d,
        s.days_start_date_to_end_date,
        p.segment AS holder_segment,
        p.holder_name
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date >= DATE '2026-01-23'
        )
      AND s.holder_name IS NOT NULL
),
status_stats AS (
    -- Avg days_start_date_to_end_date per status
    SELECT status, AVG(days_start_date_to_end_date) AS avg_days_start_date_to_end_date
    FROM screened
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_start_date_to_end_date,
        ROUND(s.days_start_date_to_end_date - g.avg_days_start_date_to_end_date, 2) AS days_start_date_to_end_date_minus_avg
    FROM screened s
    JOIN status_stats g ON g.status = s.status
    WHERE s.days_start_date_to_end_date > g.avg_days_start_date_to_end_date
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_id, status, premium
    FROM outliers
) src
PIVOT (
    MIN(premium) AS min_v, SUM(premium) AS sum_v
    FOR status IN ('ACTIVE', 'EXPIRED')
)
ORDER BY holder_id
