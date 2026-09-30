WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE kwh > 37.1
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM recent_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        quality,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, burst_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY quality ORDER BY duration_min DESC, quality, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(n_events / NULLIF(SUM(n_events) OVER (PARTITION BY quality, burst_id), 0), 4) AS n_events_share_of_quality_burst_id,
        FIRST_VALUE(n_events) OVER (PARTITION BY quality ORDER BY duration_min DESC, quality, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_events,
        ROUND(burst_id / NULLIF(SUM(burst_id) OVER (PARTITION BY quality, burst_id), 0), 4) AS burst_id_share_of_quality_burst_id,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', session_start), burst_id ORDER BY duration_min NULLS LAST) AS duration_min_rank
    FROM per_session
),
leaders AS (
    -- First row per group
    SELECT
        quality,
        burst_id,
        session_start,
        session_end,
        n_events,
        duration_min,
        running_count,
        n_events_share_of_quality_burst_id,
        first_n_events,
        burst_id_share_of_quality_burst_id
    FROM with_trends
    WHERE duration_min_rank = 1
)
SELECT *
FROM (
    SELECT burst_id, session_end, quality, running_count
    FROM leaders
) src
PIVOT (
    COUNT(running_count)
    FOR quality IN ('OK', 'EST', 'BAD')
)
