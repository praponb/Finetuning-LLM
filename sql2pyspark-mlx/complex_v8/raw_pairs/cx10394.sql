WITH recent AS (
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
    WHERE device NOT IN ('tv', 'speaker')
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 60 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(prev_played_at, 2) OVER (PARTITION BY device, TO_DATE(played_at) ORDER BY prev_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_played_at,
        COUNT(*) OVER (PARTITION BY device, track_id) AS rows_in_device_track_id,
        ROUND(AVG(burst_id) OVER (PARTITION BY device, track_id), 2) AS device_track_id_avg_burst_id
    FROM sessionized
),
listener_id_stats AS (
    -- Median is_new_session per listener_id
    SELECT listener_id, PERCENTILE_APPROX(is_new_session, 0.5) AS median_is_new_session
    FROM sequenced
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_is_new_session,
        ROUND(s.is_new_session - g.median_is_new_session, 2) AS is_new_session_minus_median
    FROM sequenced s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.is_new_session >= g.median_is_new_session
)
SELECT *
FROM (
    SELECT prev_played_at, device, rows_in_device_track_id
    FROM above_average
) src
PIVOT (
    MAX(rows_in_device_track_id)
    FOR device IN ('tv', 'desktop', 'speaker', 'car')
)
ORDER BY prev_played_at
