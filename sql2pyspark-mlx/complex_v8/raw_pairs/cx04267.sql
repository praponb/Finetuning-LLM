WITH filtered AS (
    -- Keep valid rows and derive helper columns
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
    FROM filtered
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
session_summary AS (
    -- One row per session
    SELECT
        site_id,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_no
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.site_id,
        s.session_no,
        s.session_end,
        s.duration_min,
        si.site_name,
        si.zone AS site_zone
    FROM session_summary s
    JOIN sites si ON si.id = s.site_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        site_id,
        session_no,
        session_end,
        SIZE(COLLECT_SET(site_zone) OVER w_site_id_2) AS distinct_site_zone_in_site_id,
        LAG(duration_min) OVER (PARTITION BY MONTH(session_end), site_zone ORDER BY duration_min DESC NULLS FIRST, site_id, session_no) AS prev_duration_min,
        LAST_VALUE(session_no) IGNORE NULLS OVER (ORDER BY session_end DESC, site_id, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_no
    FROM with_parent
    WINDOW w_site_id_2 AS (PARTITION BY site_id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT site_id, session_no, measure, amount
FROM (
    SELECT site_id, session_no, last_known_session_no, distinct_site_zone_in_site_id
    FROM sequenced
) src
UNPIVOT (
    amount FOR measure IN (last_known_session_no, distinct_site_zone_in_site_id)
) unp
ORDER BY site_id, session_no, measure
