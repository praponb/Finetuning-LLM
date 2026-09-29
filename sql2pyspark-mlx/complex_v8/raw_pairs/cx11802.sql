WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage - kwh AS voltage_minus_kwh
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(kwh) OVER (PARTITION BY TO_DATE(prev_read_at) ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        MIN(session_id) OVER (PARTITION BY MONTH(prev_read_at), quality) AS prev_read_at_month_quality_min_session_id,
        NTH_VALUE(session_id, 2) OVER (PARTITION BY new_session_flag, site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_session_id,
        LAST_VALUE(prev_read_at) IGNORE NULLS OVER (PARTITION BY site_id, quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_read_at,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(prev_read_at) ORDER BY kwh DESC), 3) AS kwh_cume_dist,
        LAG(voltage_minus_kwh, 1, 0) OVER w_site_id_quality_by_read_at AS prev_voltage_minus_kwh
    FROM sessionized
    WINDOW w_site_id_quality_by_read_at AS (PARTITION BY site_id, quality ORDER BY read_at, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    prev_read_at,
    session_id,
    first_kwh,
    prev_read_at_month_quality_min_session_id,
    second_session_id,
    last_known_prev_read_at,
    prev_voltage_minus_kwh,
    ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 3600, 1) AS hours_prev_read_at_to_read_at
FROM with_trends
ORDER BY prev_read_at DESC
