WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND is_explicit = TRUE
),
artist_id_stats AS (
    -- Avg bpm_filled per artist_id
    SELECT artist_id, AVG(bpm_filled) AS avg_bpm_filled
    FROM cleaned_tracks
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_filled,
        ROUND(s.bpm_filled - g.avg_bpm_filled, 2) AS bpm_filled_minus_avg
    FROM cleaned_tracks s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_filled > g.avg_bpm_filled
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MIN(played_at) AS first_played_at, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM vs_group s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT track_id, MAX(ms_played) AS max_play_ms_played, COUNT(*) AS play_count_2
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM summarized_rows s
    LEFT JOIN play_stats_2 p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    title,
    moods,
    bpm_filled,
    bpm_filled_minus_avg,
    play_count,
    max_play_ms_played,
    ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
    LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
FROM with_counts
WHERE LENGTH(TRIM(title)) >= 5
