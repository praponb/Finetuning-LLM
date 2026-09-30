WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE release_date < DATE '2025-03-08'
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.artist_name,
        a.genre AS artist_genre
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_genre ORDER BY release_date) AS release_date_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, artist_genre), 0), 4) AS bpm_share_of_artist_id_artist_genre,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_genre ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_genre ORDER BY release_date, id) AS release_date_seq
    FROM with_details_artist
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        MAX(played_at) AS last_played_at_2
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.last_played_at_2
    FROM with_windows s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_played_at_2, artist_genre, bpm_share_of_artist_id_artist_genre
    FROM summarized_rows
) src
PIVOT (
    MAX(bpm_share_of_artist_id_artist_genre)
    FOR artist_genre IN ('Indie', 'Jazz', 'Pop', 'HipHop')
)
ORDER BY last_played_at_2
