WITH scoped_plays AS (
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
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(ms_played) OVER w_listener_id_prev_played_at_m AS next_ms_played,
        LAG(session_no, 3) OVER (ORDER BY session_no, id) AS prev3_session_no,
        ROUND(AVG(session_no) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', prev_played_at) ORDER BY played_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS session_no_avg_3_prior,
        ms_played - LAG(ms_played) OVER (ORDER BY session_no, id) AS ms_played_change,
        FIRST_VALUE(session_no) OVER (ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_session_no
    FROM sessionized
    WINDOW w_listener_id_prev_played_at_m AS (PARTITION BY listener_id, DATE_TRUNC('MONTH', prev_played_at) ORDER BY played_at, id)
),
source_stats AS (
    -- Avg session_no_avg_3_prior per source
    SELECT source, AVG(session_no_avg_3_prior) AS avg_session_no_avg_3_prior
    FROM metrics
    GROUP BY source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_session_no_avg_3_prior,
        ROUND(s.session_no_avg_3_prior - g.avg_session_no_avg_3_prior, 2) AS session_no_avg_3_prior_minus_avg
    FROM metrics s
    JOIN source_stats g ON g.source = s.source
    WHERE s.session_no_avg_3_prior > g.avg_session_no_avg_3_prior
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    prev_played_at,
    new_session_flag,
    session_no,
    next_ms_played,
    prev3_session_no,
    session_no_avg_3_prior,
    ms_played_change,
    first_session_no,
    avg_session_no_avg_3_prior,
    session_no_avg_3_prior_minus_avg
FROM vs_group
WHERE track_id IS NOT NULL
ORDER BY session_no_avg_3_prior DESC, id
LIMIT 5
