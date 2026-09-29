WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        ABS(bpm - 100) AS bpm_distance
    FROM tracks
    WHERE title IS NOT NULL
      AND is_explicit = TRUE
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count_2
    FROM filtered s
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.release_date_label,
        s.bpm_distance,
        s.track_count,
        s.last_track_release_date,
        s.track_count_2,
        a.genre AS artist_genre
    FROM with_stats s
    LEFT JOIN artists a ON a.id = s.artist_id
),
unique_rows AS (
    -- Keep the latest row per artist_id, artist_genre
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        release_date_label,
        bpm_distance,
        track_count,
        last_track_release_date,
        track_count_2,
        artist_genre
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id, artist_genre ORDER BY last_track_release_date DESC, id DESC) AS rn
        FROM with_details_artist
    ) ranked
    WHERE rn = 1
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.track_count,
        s.last_track_release_date,
        s.track_count_2,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM unique_rows s
    JOIN plays p ON p.track_id = s.id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        is_explicit,
        track_count,
        play_id,
        play_listener_id,
        play_source,
        play_ms_played,
        play_played_at,
        ABS(bpm - 108) AS bpm_distance
    FROM with_children_plays
),
with_details AS (
    -- Join plays
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.track_count,
        s.play_id,
        s.play_listener_id,
        s.play_source,
        s.play_ms_played,
        s.play_played_at,
        s.bpm_distance,
        p.track_id AS play_track_id,
        p.listener_id AS play_listener_id_2,
        l.plan AS listener_plan
    FROM derived s
    LEFT JOIN plays p ON p.id = s.play_id
    LEFT JOIN listeners l ON l.id = p.listener_id
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    track_count,
    play_listener_id,
    play_played_at,
    listener_plan,
    DATE_FORMAT(play_played_at, 'yyyy-MM-dd') AS play_played_at_label
FROM with_details
ORDER BY id, play_listener_id DESC
