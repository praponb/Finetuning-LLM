WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-03-08'
),
with_prev AS (
    -- Previous played_at_plus_15m per source
    SELECT *, LAG(played_at_plus_15m) OVER (PARTITION BY source ORDER BY played_at_plus_15m, id) AS prev_played_at_plus_15m
    FROM cleaned_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at_plus_15m IS NULL OR played_at_plus_15m > prev_played_at_plus_15m + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY source ORDER BY played_at_plus_15m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        ms_played,
        is_skipped,
        source,
        played_at_plus_15m,
        ROUND(AVG(ms_played) OVER (PARTITION BY TO_DATE(played_at_plus_15m) ORDER BY ms_played ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        ROUND(AVG(starts_session) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at_plus_15m), session_id), 2) AS played_at_plus_15m_month_session_id_avg_starts_s,
        MAX(session_id) OVER (PARTITION BY TO_DATE(played_at_plus_15m) ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_session_id_so_far,
        LEAD(starts_session, 2, 0) OVER (PARTITION BY TO_DATE(played_at_plus_15m) ORDER BY ms_played ASC, id) AS next2_starts_session
    FROM sessionized
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT is_skipped, played_at_plus_15m, source, ms_played_avg_2_prior
    FROM with_windows
) src
PIVOT (
    COUNT(ms_played_avg_2_prior)
    FOR source IN ('radio', 'playlist', 'search', 'album')
)
