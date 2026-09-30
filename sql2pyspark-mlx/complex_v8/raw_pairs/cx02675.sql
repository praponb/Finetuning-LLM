WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        DAYOFWEEK(release_date) AS release_date_dayofweek,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'workout')
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.is_skipped = TRUE THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_is_skipped_count
    FROM recent_tracks s
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.days_since_release_date,
        s.release_date_dayofweek,
        s.title_ref,
        s.play_is_skipped_count,
        a.genre AS artist_genre
    FROM summarized_rows s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'Jazz'
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.release_date_dayofweek,
        s.title_ref,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM enriched_artist s
    JOIN plays p ON p.track_id = s.id AND p.device IN ('tv', 'desktop', 'phone')
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM exploded_detail s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND ARRAY_CONTAINS(t.moods, 'party')
              AND t.duration_sec > 183
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    release_date_dayofweek,
    title_ref,
    play_id,
    play_played_at,
    SUBSTRING(TRIM(title_ref), 1, 4) AS title_ref_prefix,
    ADD_MONTHS(release_date, 6) AS release_date_plus_6m
FROM qualified
