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
        moods,
        COALESCE(bpm, 0) AS bpm_filled,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.bpm_filled,
        s.is_explicit_label,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM cleaned_tracks s
    LEFT JOIN plays p ON p.track_id = s.id
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year,
        p.device AS play_device,
        p.ms_played AS play_ms_played_2
    FROM with_children_plays s
    LEFT JOIN artists a ON a.id = s.artist_id
    LEFT JOIN plays p ON p.id = s.play_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(artist_country, 'UNKNOWN') AS artist_country_or_unknown,
        CONCAT(play_device, '#', CAST(play_ms_played AS STRING)) AS play_device_ref
    FROM enriched_artist
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.moods,
        s.bpm_filled,
        s.play_id,
        s.play_ms_played,
        s.play_source,
        s.artist_country,
        s.artist_debut_year,
        s.play_device,
        s.play_ms_played_2,
        s.artist_country_or_unknown,
        s.play_device_ref,
        a.genre AS artist_genre
    FROM tagged s
    JOIN artists a ON a.id = s.artist_id
),
enriched_artist_2 AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.moods,
        s.bpm_filled,
        s.play_id,
        s.play_ms_played,
        s.play_source,
        s.artist_country,
        s.artist_debut_year,
        s.artist_country_or_unknown,
        s.artist_genre,
        a.country AS artist_country_2,
        a.is_verified AS artist_is_verified
    FROM with_details_artist s
    JOIN artists a ON a.id = s.artist_id
),
joined_play AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.play_id,
        s.play_ms_played,
        s.play_source,
        s.artist_country,
        s.artist_genre,
        s.artist_country_2,
        s.artist_is_verified,
        p.track_id AS play_track_id,
        p.listener_id AS play_listener_id
    FROM enriched_artist_2 s
    JOIN plays p ON p.id = s.play_id
)
SELECT
    id,
    artist_id,
    title,
    play_ms_played,
    play_source,
    artist_country,
    artist_country_2,
    play_listener_id,
    COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown,
    FLOOR(play_ms_played / 50000) * 50000 AS play_ms_played_bucket
FROM joined_play
ORDER BY play_ms_played DESC
