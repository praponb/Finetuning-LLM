WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        INITCAP(TRIM(title)) AS title_clean,
        ARRAY_CONTAINS(moods, 'party') AS has_party,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE artist_id IS NOT NULL
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_clean,
        s.has_party,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM recent_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played
    FROM enriched_artist s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.debut_year AS artist_debut_year_2,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM profiled s
    JOIN artists a ON a.id = s.artist_id AND UPPER(TRIM(a.artist_name)) LIKE 'P%'
    WHERE a.country IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_clean,
        s.has_party,
        s.artist_debut_year,
        s.avg_play_ms_played,
        s.artist_debut_year_2,
        s.artist_is_verified,
        a.is_verified AS artist_is_verified_2
    FROM with_parent s
    LEFT JOIN artists a ON a.id = s.artist_id
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE source = 'radio') AS play_source_count,
        MIN(played_at) AS first_played_at,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        p.first_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM joined s
    LEFT JOIN play_stats_2 p ON p.track_id = s.id
),
combined_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.title_clean,
        s.artist_debut_year,
        s.avg_play_ms_played,
        s.artist_debut_year_2,
        s.play_source_count,
        s.first_played_at,
        s.play_count,
        a.is_verified AS artist_is_verified_3,
        a.artist_name
    FROM summarized_rows s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    bpm,
    title_clean,
    avg_play_ms_played,
    artist_debut_year_2,
    play_source_count,
    first_played_at,
    play_count,
    artist_name,
    first_played_at + INTERVAL 7 DAYS AS first_played_at_plus_7d,
    MONTH(first_played_at) AS first_played_at_month
FROM combined_details_artist
WHERE artist_debut_year_2 < 2015
ORDER BY bpm DESC, artist_debut_year_2, id
LIMIT 5
