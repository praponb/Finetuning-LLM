WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN kwh >= 81.9 THEN 'HIGH' WHEN kwh >= 35.9 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier
    FROM meter_readings
    WHERE kwh > 30.2
),
with_prev AS (
    -- Previous read_at per kwh_tier
    SELECT *, LAG(read_at) OVER (PARTITION BY kwh_tier ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY kwh_tier ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        kwh_tier,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        SUM(kwh) AS session_kwh,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY kwh_tier, session_id
),
annotated AS (
    -- Label and score rows
    SELECT
        kwh_tier,
        session_id,
        session_start,
        session_end,
        events,
        session_kwh,
        duration_min,
        CASE WHEN kwh_tier IN ('LOW', 'MEDIUM') THEN 'group_a' WHEN kwh_tier = 'HIGH' THEN 'group_b' ELSE 'other' END AS kwh_tier_group,
        session_start + INTERVAL 15 MINUTES AS session_start_plus_15m
    FROM per_session
    WHERE session_end >= TIMESTAMP '2025-01-25 09:00:00'
)
SELECT
    kwh_tier,
    session_start,
    events,
    session_kwh,
    kwh_tier_group
FROM annotated
ORDER BY kwh_tier_group
