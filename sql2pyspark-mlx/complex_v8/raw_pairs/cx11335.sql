WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM prepared_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.quality_prefix,
        s.prev_read_at,
        s.is_new_session,
        s.session_id,
        si.site_name
    FROM sessionized s
    JOIN sites si ON si.id = s.site_id
    WHERE UPPER(TRIM(si.site_name)) LIKE 'R%'
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    is_new_session,
    site_name,
    GREATEST(session_id, is_new_session) AS greatest_session_id_is_new_session
FROM enriched
ORDER BY site_id DESC, read_at
