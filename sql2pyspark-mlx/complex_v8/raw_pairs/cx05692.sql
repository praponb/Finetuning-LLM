WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE kwh BETWEEN 12.2 AND 117.2
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality_ref,
        s.prev_read_at,
        s.starts_session,
        s.session_no,
        si.site_name,
        si.tariff AS site_tariff
    FROM sessionized s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IS NOT NULL
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality_ref,
    prev_read_at,
    starts_session,
    session_no,
    site_tariff
FROM combined_details_site
WHERE voltage BETWEEN 221.4 AND 242.3
ORDER BY quality_ref DESC, read_at DESC
