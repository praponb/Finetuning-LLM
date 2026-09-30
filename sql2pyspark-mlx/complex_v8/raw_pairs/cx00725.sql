WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm,
        ADD_MONTHS(release_date, 3) AS release_date_plus_3m
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 8
      AND duration_sec >= 209
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(ms_played) AS max_play_ms_played,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        p.last_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM cleaned s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        bpm,
        last_played_at,
        bpm - LAG(bpm) OVER (PARTITION BY play_count ORDER BY bpm, id) AS bpm_change,
        ROW_NUMBER() OVER (PARTITION BY play_count ORDER BY bpm, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit
    FROM with_counts
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(played_at) AS first_played_at,
        COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) AS play_is_skipped_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count
    FROM with_windows s
    LEFT JOIN play_stats_2 p ON p.track_id = s.id
),
with_windows_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        bpm - LAG(bpm) OVER (PARTITION BY play_is_skipped_count, DATE_TRUNC('MONTH', release_date) ORDER BY seq_no, id) AS bpm_change_2,
        COUNT(*) OVER (PARTITION BY seq_no) AS rows_in_seq_no,
        DENSE_RANK() OVER (PARTITION BY play_is_skipped_count, DATE_TRUNC('MONTH', release_date) ORDER BY seq_no) AS seq_no_dense_rank
    FROM with_stats
)
-- Final projection
SELECT
    id,
    title,
    release_date,
    is_explicit,
    bpm,
    last_played_at,
    bpm_change,
    first_played_at,
    play_is_skipped_count,
    bpm_change_2,
    rows_in_seq_no,
    seq_no_dense_rank
FROM with_windows_2
WHERE TO_DATE(last_played_at) < DATE '2025-02-05'
ORDER BY bpm_change_2, id
LIMIT 20
