WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        ABS(duration_sec - 239) AS duration_sec_distance,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count_2,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS first_track_release_date
    FROM base_tracks s
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.days_since_release_date,
        s.duration_sec_distance,
        s.track_count,
        s.track_count_2,
        s.first_track_release_date,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM with_stats s
    JOIN artists a ON a.id = s.artist_id AND a.debut_year <= 2006
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(duration_sec_distance / NULLIF(SUM(duration_sec_distance) OVER (PARTITION BY is_explicit, artist_genre), 0), 4) AS duration_sec_distance_share_of_is_explicit_artis,
        ROW_NUMBER() OVER w_track_count_by_track_count AS track_count_seq
    FROM with_parent
    WINDOW w_track_count_by_track_count AS (PARTITION BY track_count ORDER BY track_count ASC, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_count_2, is_explicit, artist_genre, track_count_seq
    FROM sequenced
) src
PIVOT (
    COUNT(track_count_seq)
    FOR artist_genre IN ('HipHop', 'Pop')
)
ORDER BY track_count_2, is_explicit
