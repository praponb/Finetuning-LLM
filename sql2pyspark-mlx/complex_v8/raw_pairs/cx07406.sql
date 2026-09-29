WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-12'
      AND device NOT IN ('speaker', 'car')
),
with_details_track AS (
    -- Join tracks
    SELECT s.*, t.duration_sec AS track_duration_sec, t.title AS track_title
    FROM scoped_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.duration_sec >= 142
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(played_at) AS first_played_at,
        SUM(ms_played) AS total_play_ms_played
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        p.total_play_ms_played
    FROM with_details_track s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM summarized_rows
    WHERE total_play_ms_played > (SELECT AVG(total_play_ms_played) FROM summarized_rows)
)
-- Subtotals with GROUPING SETS
SELECT
    is_skipped,
    source,
    listener_id,
    SUM(total_play_ms_played) AS total_total_play_ms_played,
    MAX(track_duration_sec) AS max_track_duration_sec
FROM without_matches
GROUP BY GROUPING SETS ((is_skipped, source, listener_id), (is_skipped), (source), ())
