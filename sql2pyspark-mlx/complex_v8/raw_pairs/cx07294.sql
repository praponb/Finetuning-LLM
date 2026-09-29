WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE is_skipped = FALSE
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM valid_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(CASE WHEN source = 'search' THEN 1 END) AS play_source_count,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        COALESCE(p.play_count, 0) AS play_count
    FROM sessionized s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_device_by_starts_session_tie, 3) AS starts_session_percent_rank,
        ROW_NUMBER() OVER (ORDER BY ms_played_bucket DESC NULLS FIRST, id) AS seq_no,
        LEAD(play_source_count, 2) OVER (ORDER BY ms_played_bucket DESC NULLS FIRST, id) AS next2_play_source_count,
        MIN(play_count) OVER (PARTITION BY device) AS device_min_play_count,
        ROUND(AVG(play_count) OVER (PARTITION BY device), 2) AS device_avg_play_count
    FROM summarized_rows
    WINDOW w_device_by_starts_session_tie AS (PARTITION BY device ORDER BY starts_session DESC)
)
SELECT id, metric_name, amount
FROM (
    SELECT id, starts_session_percent_rank, device_avg_play_count
    FROM with_trends
) src
UNPIVOT (
    amount FOR metric_name IN (starts_session_percent_rank, device_avg_play_count)
) unp
