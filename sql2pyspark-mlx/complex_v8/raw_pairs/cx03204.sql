WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE artist_id >= 10
      AND bpm <= 143
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count_2
    FROM cleaned_tracks s
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.days_since_release_date,
        s.bpm_per_duration_sec,
        s.track_count,
        s.track_count_2,
        t.mood
    FROM profiled s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Subtotals with GROUPING SETS
SELECT
    track_count_2,
    mood,
    track_count,
    SUM(track_count_2) AS total_track_count_2,
    COUNT(*) AS n_rows
FROM per_label
GROUP BY GROUPING SETS ((track_count_2, mood, track_count), (track_count_2), (mood))
ORDER BY track_count_2 NULLS LAST, mood NULLS LAST, track_count NULLS LAST
