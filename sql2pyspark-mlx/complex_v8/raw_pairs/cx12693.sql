WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched AS (
    -- Join sites
    SELECT s.*, si.zone AS site_zone
    FROM sessionized s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IN ('Z4', 'Z3')
)
-- Final projection
SELECT
    id,
    kwh,
    new_session_flag,
    burst_id,
    site_zone,
    CASE WHEN new_session_flag >= 1 THEN 'large' WHEN new_session_flag >= 0 THEN 'medium' ELSE 'small' END AS new_session_flag_tier,
    SUBSTRING(TRIM(site_zone), 1, 4) AS site_zone_prefix
FROM enriched
ORDER BY kwh, new_session_flag_tier NULLS LAST
