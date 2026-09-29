WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        duration_sec - bpm AS duration_sec_minus_bpm,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
    WHERE NOT is_explicit
      AND release_date IS NOT NULL
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.duration_sec), 2) FROM tracks t WHERE t.artist_id = s.artist_id) AS avg_track_duration_sec
    FROM recent s
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        duration_sec_minus_bpm,
        title_prefix,
        avg_track_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm DESC, id DESC) AS rn
        FROM profiled
    ) ranked
    WHERE rn = 1
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM latest s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.duration_sec_minus_bpm,
        s.title_prefix,
        s.avg_track_duration_sec,
        s.last_played_at,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM with_stats s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    duration_sec,
    release_date,
    duration_sec_minus_bpm,
    title_prefix,
    avg_track_duration_sec,
    last_played_at,
    artist_debut_year,
    artist_is_verified,
    DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
    CONCAT_WS(': ', artist_genre, title_prefix) AS artist_genre_title_prefix_label
FROM with_details
WHERE artist_id IS NOT NULL
ORDER BY title_prefix, artist_genre_title_prefix_label DESC
