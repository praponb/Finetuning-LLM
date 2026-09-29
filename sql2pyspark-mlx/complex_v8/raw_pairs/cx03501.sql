WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE NOT is_explicit
      AND SIZE(moods) >= 1
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.bpm), 2) FROM tracks t WHERE t.artist_id = s.artist_id) AS avg_track_bpm,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS first_track_release_date
    FROM prepared s
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.avg_track_bpm,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM with_counts s
    JOIN plays p ON p.track_id = s.id
),
with_details AS (
    -- Join artists
    SELECT
        s.*,
        a.artist_name,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM with_children_plays s
    LEFT JOIN artists a ON a.id = s.artist_id
),
artist_country_stats AS (
    -- Avg play_ms_played per artist_country
    SELECT artist_country, AVG(play_ms_played) AS avg_play_ms_played
    FROM with_details
    GROUP BY artist_country
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_play_ms_played,
        ROUND(s.play_ms_played - g.avg_play_ms_played, 2) AS play_ms_played_minus_avg
    FROM with_details s
    JOIN artist_country_stats g ON g.artist_country = s.artist_country
    WHERE s.play_ms_played > g.avg_play_ms_played
),
derived AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(play_source)) AS play_source_len
    FROM above_average
    WHERE avg_play_ms_played > 44500.0
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    avg_track_bpm,
    play_id,
    play_listener_id,
    artist_country,
    artist_debut_year,
    avg_play_ms_played,
    play_ms_played_minus_avg
FROM derived
ORDER BY play_id DESC
