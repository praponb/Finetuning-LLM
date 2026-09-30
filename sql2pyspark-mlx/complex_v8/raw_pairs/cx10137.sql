WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-06'
),
with_prev AS (
    -- Previous played_at per device
    SELECT *, LAG(played_at) OVER (PARTITION BY device ORDER BY played_at, id) AS prev_played_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
listener_id_stats AS (
    -- Avg burst_id per listener_id
    SELECT listener_id, AVG(burst_id) AS avg_burst_id
    FROM sessionized
    GROUP BY listener_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_burst_id,
        ROUND(s.burst_id - g.avg_burst_id, 2) AS burst_id_minus_avg
    FROM sessionized s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.burst_id > g.avg_burst_id
)
SELECT *
FROM (
    SELECT new_session_flag, listener_id, source, burst_id_minus_avg
    FROM compared
) src
PIVOT (
    SUM(burst_id_minus_avg)
    FOR source IN ('playlist', 'album', 'search', 'radio')
)
