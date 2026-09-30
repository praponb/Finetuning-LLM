WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE quality <> 'OK'
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        site_id,
        session_no,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_no
),
enriched_site AS (
    -- Enrich with dimension data
    SELECT
        s.site_id,
        s.session_no,
        s.session_start,
        s.session_end,
        s.event_count,
        s.session_kwh,
        s.duration_min,
        si.commissioned_on AS site_commissioned_on,
        si.site_name,
        si.zone AS site_zone
    FROM session_summary s
    LEFT JOIN sites si ON si.id = s.site_id
),
unique_rows AS (
    -- Keep the latest row per site_zone
    SELECT
        site_id,
        session_no,
        session_start,
        session_end,
        event_count,
        session_kwh,
        duration_min,
        site_commissioned_on,
        site_name,
        site_zone
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_zone ORDER BY session_no DESC, site_id DESC, session_no DESC) AS recency_rank
        FROM enriched_site
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    site_id,
    session_start,
    session_end,
    event_count,
    session_kwh,
    duration_min,
    site_commissioned_on,
    site_name,
    site_zone,
    DATE_ADD(site_commissioned_on, 90) AS site_commissioned_on_plus_90d
FROM unique_rows
WHERE event_count < 4
ORDER BY session_kwh DESC
