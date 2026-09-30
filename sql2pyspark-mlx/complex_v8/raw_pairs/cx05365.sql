WITH recent AS (
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
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM recent
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
per_session AS (
    -- One row per session
    SELECT
        quality,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(voltage) AS session_voltage,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY session_id ORDER BY event_count DESC, quality, session_id), 3) AS event_count_cume_dist,
        LAST_VALUE(session_voltage) IGNORE NULLS OVER (PARTITION BY session_id ORDER BY event_count DESC, quality, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_voltage,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY session_voltage DESC, quality, session_id) AS session_voltage_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY session_id, event_count ORDER BY event_count), 3) AS event_count_cume_dist_2,
        MAX(session_voltage) OVER (PARTITION BY session_id ORDER BY event_count DESC, quality, session_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS session_voltage_max_6_prior
    FROM per_session
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE event_count_cume_dist > (SELECT AVG(event_count_cume_dist) FROM windowed)
)
SELECT
    quality,
    session_id,
    session_start,
    session_end,
    event_count,
    session_voltage,
    duration_min,
    event_count_cume_dist_2,
    session_voltage_max_6_prior,
    duration_min + session_voltage AS duration_min_plus_session_voltage
FROM qualified
WHERE last_known_session_voltage <= 749.0
ORDER BY duration_min_plus_session_voltage DESC NULLS FIRST, quality
