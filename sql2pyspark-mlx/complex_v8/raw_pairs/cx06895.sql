WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE status = 'LAPSED'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(start_date, 3) OVER (PARTITION BY status_or_unknown, status ORDER BY end_date DESC, id) AS next3_start_date,
        LAST_VALUE(premium_bucket) IGNORE NULLS OVER (PARTITION BY status_or_unknown, status ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_bucket,
        LAG(start_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id) AS prev2_start_date,
        MAX(days_since_start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_start_date_max_3_prior,
        ROUND(AVG(days_since_start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date) ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_start_date_avg_7
    FROM prepared
)
SELECT
    holder_id,
    status_or_unknown,
    status,
    GROUPING_ID() AS gid,
    MAX(days_since_start_date_max_3_prior) AS max_days_since_start_date_max_3_prior,
    MIN(next3_start_date) AS first_next3_start_date
FROM with_windows
GROUP BY GROUPING SETS ((holder_id, status_or_unknown, status), (holder_id))
