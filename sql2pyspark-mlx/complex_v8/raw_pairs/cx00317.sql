WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        site_id,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY site_id, session_id
),
with_parent AS (
    -- Join sites
    SELECT
        s.site_id,
        s.session_id,
        s.session_start,
        s.session_end,
        s.events,
        s.session_kwh,
        s.duration_min,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM session_summary s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_site_id_session_id_by_site_c AS site_commissioned_on_rank,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY site_id, events)) AS distinct_site_tariff_in_site_id_events,
        ROW_NUMBER() OVER (PARTITION BY site_id, site_tariff ORDER BY duration_min, site_id, session_id) AS duration_min_seq
    FROM with_parent
    WINDOW w_site_id_session_id_by_site_c AS (PARTITION BY site_id, session_id ORDER BY site_commissioned_on NULLS LAST, site_id, session_id)
)
SELECT
    site_id,
    session_id,
    session_start,
    session_end,
    duration_min,
    site_tariff,
    site_installed_kw,
    site_commissioned_on,
    site_commissioned_on_rank,
    distinct_site_tariff_in_site_id_events,
    duration_min_seq,
    session_start + INTERVAL 30 MINUTES AS session_start_plus_30m,
    QUARTER(site_commissioned_on) AS site_commissioned_on_quarter
FROM with_windows
ORDER BY site_tariff DESC, site_installed_kw DESC, site_id, session_id
LIMIT 10
