WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source
    FROM plays
    WHERE track_id <= 25
      AND listener_id IS NOT NULL
),
joined AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM filtered_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ARRAY_CONTAINS(track_moods, 'focus') AS has_focus,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        IF(artist_is_verified, 'yes', 'no') AS artist_is_verified_label
    FROM joined
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    track_moods,
    track_artist_id,
    artist_country,
    has_focus,
    source_or_unknown,
    artist_is_verified_label,
    CONCAT(artist_country, '#', CAST(track_id AS STRING)) AS artist_country_ref,
    UPPER(TRIM(artist_country)) AS artist_country_clean
FROM scored
ORDER BY artist_country DESC, source_or_unknown DESC
