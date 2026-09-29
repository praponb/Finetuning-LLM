WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
    WHERE start_date < DATE '2025-05-30'
      AND end_date IS NOT NULL
),
holder_id_stats AS (
    -- Median days_start_date_to_end_date per holder_id
    SELECT holder_id, PERCENTILE_APPROX(days_start_date_to_end_date, 0.5) AS median_days_start_date_to_end_date
    FROM recent_policys
    GROUP BY holder_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_start_date_to_end_date,
        ROUND(s.days_start_date_to_end_date - g.median_days_start_date_to_end_date, 2) AS days_start_date_to_end_date_minus_median
    FROM recent_policys s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.days_start_date_to_end_date >= g.median_days_start_date_to_end_date
)
SELECT
    COALESCE(status, 'ALL') AS status,
    holder_id,
    SUM(days_start_date_to_end_date) FILTER (WHERE status NOT IN ('EXPIRED', 'CANCELLED')) AS days_start_date_to_end_date_status_not_in_expire
FROM compared
GROUP BY GROUPING SETS ((status, holder_id), (status), (holder_id))
