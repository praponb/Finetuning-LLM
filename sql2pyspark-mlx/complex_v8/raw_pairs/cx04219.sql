WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
    WHERE voltage <= 237.5
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_group AS (
    -- Summarise per group
    SELECT
        session_no,
        DATE_TRUNC('MONTH', prev_read_at) AS prev_read_at_month,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM sessionized
    GROUP BY session_no, DATE_TRUNC('MONTH', prev_read_at)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE session_no > (SELECT AVG(session_no) FROM per_group)
)
-- Final projection
SELECT
    session_no,
    prev_read_at_month,
    n_rows,
    distinct_site_ids,
    n_rows + distinct_site_ids AS n_rows_plus_distinct_site_ids,
    QUARTER(prev_read_at_month) AS prev_read_at_month_quarter
FROM without_matches
ORDER BY session_no DESC, prev_read_at_month_quarter DESC NULLS FIRST
