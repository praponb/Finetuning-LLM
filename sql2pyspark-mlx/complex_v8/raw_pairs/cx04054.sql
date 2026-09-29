WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    site_id,
    session_no,
    quality,
    GROUPING_ID() AS gid,
    MIN(kwh) AS min_kwh,
    ROUND(AVG(is_new_session), 2) AS avg_is_new_session
FROM sessionized
GROUP BY GROUPING SETS ((site_id, session_no, quality), (site_id))
ORDER BY site_id NULLS LAST, session_no NULLS LAST, quality NULLS LAST
