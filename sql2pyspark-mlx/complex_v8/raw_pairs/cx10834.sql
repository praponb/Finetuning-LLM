WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        TRUNC(end_date, 'MM') AS end_date_month_start,
        MONTH(start_date) AS start_date_month
    FROM policies
    WHERE product NOT IN ('LIFE', 'TRAVEL')
      AND status = 'EXPIRED'
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_policys
    WHERE start_date_month < (SELECT MAX(start_date_month) FROM filtered_policys)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        start_date,
        status,
        end_date_month_start,
        start_date_month,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY start_date_month, status)) AS distinct_holder_id_in_start_date_month_status,
        ROUND(start_date_month - AVG(start_date_month) OVER w_start_date_month_status, 2) AS start_date_month_vs_start_date_month_status_avg,
        COUNT(*) OVER (PARTITION BY start_date_month, status) AS rows_in_start_date_month_status
    FROM screened
    WINDOW w_start_date_month_status AS (PARTITION BY start_date_month, status)
)
SELECT id, metric_name, value
FROM (
    SELECT id, start_date_month, distinct_holder_id_in_start_date_month_status
    FROM with_windows
) src
UNPIVOT (
    value FOR metric_name IN (start_date_month, distinct_holder_id_in_start_date_month_status)
) unp
