WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM base_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
quality_stats AS (
    -- Avg new_session_flag per quality
    SELECT quality, AVG(new_session_flag) AS avg_new_session_flag
    FROM sessionized
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_new_session_flag,
        ROUND(s.new_session_flag - g.avg_new_session_flag, 2) AS new_session_flag_minus_avg
    FROM sessionized s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.new_session_flag > g.avg_new_session_flag
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality_group,
        s.prev_read_at,
        s.session_id,
        s.new_session_flag_minus_avg,
        si.installed_kw AS site_installed_kw
    FROM above_average s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT session_id, quality_group, new_session_flag_minus_avg
    FROM with_details
) src
PIVOT (
    SUM(new_session_flag_minus_avg)
    FOR quality_group IN ('group_a', 'group_b')
)
ORDER BY session_id
