WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        QUARTER(release_date) AS release_date_quarter,
        UPPER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE LOWER(title) LIKE '%pa%'
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM prepared_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id IS NOT NULL
        )
      AND ARRAY_CONTAINS(s.moods, 'chill')
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        s.title_clean,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM with_activity s
    LEFT JOIN plays p ON p.track_id = s.id
),
latest AS (
    -- Keep the latest row per play_device, artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        title_clean,
        play_id,
        play_listener_id,
        play_played_at,
        play_device
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_device, artist_id ORDER BY release_date DESC, id DESC, play_id DESC) AS recency_rank
        FROM exploded_detail
    ) ranked
    WHERE recency_rank = 1
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.title_clean,
        s.play_id,
        s.play_listener_id,
        s.play_played_at,
        p.id AS play_id_2,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at_2,
        p.listener_id AS play_listener_id_2,
        p.source AS play_source
    FROM latest s
    JOIN plays p ON p.track_id = s.id
)
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    title_clean,
    play_id,
    play_played_at,
    play_id_2,
    play_ms_played,
    play_played_at_2,
    play_source,
    CASE WHEN play_source IN ('radio', 'album') THEN 'group_a' WHEN play_source = 'playlist' THEN 'group_b' ELSE 'other' END AS play_source_group
FROM exploded_detail_plays
ORDER BY play_source DESC, play_ms_played DESC NULLS FIRST
