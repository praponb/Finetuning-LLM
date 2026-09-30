WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM cleaned_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.quality,
        s.prev_read_at,
        s.is_new_session,
        s.session_no,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM sessionized s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z4'
),
deduped AS (
    -- Keep the earliest row per site_zone, site_id
    SELECT
        id,
        site_id,
        quality,
        prev_read_at,
        is_new_session,
        session_no,
        site_zone,
        site_installed_kw
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone, site_id ORDER BY is_new_session, id) AS row_num
        FROM with_details_site
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    is_new_session,
    quality,
    MAX(session_no) AS max_session_no
FROM deduped
GROUP BY CUBE(is_new_session, quality)
ORDER BY is_new_session NULLS LAST, quality NULLS LAST
