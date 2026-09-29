WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
without_matches AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM sessionized s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.installed_kw > 141.6
        )
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    quality_len,
    prev_read_at,
    starts_session
FROM without_matches
ORDER BY prev_read_at, quality_len
