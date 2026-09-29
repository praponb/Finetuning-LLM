WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        LENGTH(TRIM(device)) AS device_len,
        CONCAT_WS(' / ', device, source) AS device_source_label,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 08:00:00'
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.device_source_label,
        s.played_at_label,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date,
        l.username AS listener_username,
        l.birth_year AS listener_birth_year
    FROM cleaned s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        ms_played,
        played_at_label,
        track_is_explicit,
        listener_username,
        listener_birth_year,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY track_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        RANK() OVER w_all_by_track_release_date AS track_release_date_rank
    FROM with_parent
    WINDOW w_all_by_track_release_date AS (ORDER BY track_release_date DESC, id)
),
enriched_track AS (
    -- Attach track attributes
    SELECT
        s.*,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit_2,
        t.artist_id AS track_artist_id,
        a.artist_name,
        a.is_verified AS artist_is_verified
    FROM windowed s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
    WHERE NOT t.is_explicit
),
with_parent_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.track_id,
        s.ms_played,
        s.played_at_label,
        s.track_is_explicit,
        s.track_release_date_rank,
        s.track_moods,
        s.track_is_explicit_2,
        s.track_artist_id,
        s.artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM enriched_track s
    LEFT JOIN artists a ON a.id = s.track_artist_id
),
combined_details AS (
    -- Join tracks
    SELECT
        s.id,
        s.track_id,
        s.track_moods,
        s.track_is_explicit_2,
        s.track_artist_id,
        s.artist_debut_year,
        t.title AS track_title,
        a.artist_name,
        a.genre AS artist_genre
    FROM with_parent_2 s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = s.track_artist_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(artist_genre)) AS artist_genre_clean,
        LOWER(REGEXP_REPLACE(TRIM(artist_genre), ' ', '_')) AS artist_genre_slug,
        CASE WHEN artist_genre IN ('HipHop', 'Jazz') THEN 'group_a' WHEN artist_genre = 'Classical' THEN 'group_b' ELSE 'other' END AS artist_genre_group
    FROM combined_details
)
SELECT
    id,
    track_id,
    track_moods,
    track_is_explicit_2,
    track_artist_id,
    artist_debut_year,
    track_title,
    artist_name,
    artist_genre_clean,
    artist_genre_slug,
    artist_genre_group
FROM enriched
