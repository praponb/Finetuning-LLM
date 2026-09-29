WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
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
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
deduped AS (
    -- Keep the earliest row per quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        prev_read_at,
        is_new_session,
        session_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality ORDER BY kwh, id) AS rn
        FROM sessionized
    ) ranked
    WHERE rn = 1
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.prev_read_at,
        s.is_new_session,
        s.session_id,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM deduped s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prev_read_at,
    is_new_session,
    session_id,
    reading_id,
    reading_kwh,
    DATEDIFF(DATE '2025-12-31', prev_read_at) AS days_since_prev_read_at
FROM exploded_detail
