WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE bpm BETWEEN 75 AND 142
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        duration_sec,
        release_date,
        bpm - duration_sec AS bpm_minus_duration_sec
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        release_date,
        bpm_minus_duration_sec,
        bpm_minus_duration_sec - LAG(bpm_minus_duration_sec) OVER (ORDER BY release_date ASC, id) AS bpm_minus_duration_sec_change,
        DENSE_RANK() OVER (ORDER BY release_date ASC) AS release_date_dense_rank
    FROM labeled
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.duration_sec,
        s.bpm_minus_duration_sec,
        s.release_date_dense_rank,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM metrics s
    JOIN plays p ON p.track_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    play_id,
    play_listener_id,
    PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec,
    PERCENTILE_APPROX(play_ms_played, 0.5) AS median_play_ms_played
FROM line_level
GROUP BY GROUPING SETS ((play_id, play_listener_id), (play_id), (play_listener_id), ())
ORDER BY play_id NULLS LAST, play_listener_id NULLS LAST
