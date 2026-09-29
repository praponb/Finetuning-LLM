WITH prepared AS (
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
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        INITCAP(TRIM(title)) AS title_clean,
        ADD_MONTHS(release_date, 3) AS release_date_plus_3m
    FROM tracks
    WHERE artist_id IS NOT NULL
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.is_explicit_label,
        s.title_clean,
        s.release_date_plus_3m,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.source AS play_source
    FROM prepared s
    JOIN plays p ON p.track_id = s.id
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        bpm,
        is_explicit_label,
        title_clean,
        release_date_plus_3m,
        play_id,
        play_listener_id,
        ABS(bpm - 108) AS bpm_distance
    FROM with_children_plays
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.is_explicit_label,
        s.play_id,
        s.play_listener_id,
        l.interests AS listener_interests,
        l.plan AS listener_plan,
        l.username AS listener_username,
        p.source AS play_source
    FROM derived s
    JOIN listeners l ON l.id = s.play_listener_id
    LEFT JOIN plays p ON p.id = s.play_id
)
SELECT
    id,
    artist_id,
    title,
    bpm,
    play_id,
    play_listener_id,
    listener_plan,
    listener_username,
    FLOOR(bpm / 25) * 25 AS bpm_bucket
FROM combined_details_listener
