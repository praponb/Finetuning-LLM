WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date
    FROM recent s
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
        s.track_count,
        s.last_track_release_date,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped
    FROM summarized_rows s
    LEFT JOIN plays p ON p.track_id = s.id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        release_date,
        track_count,
        last_track_release_date,
        play_id,
        play_listener_id,
        play_is_skipped,
        LENGTH(TRIM(title)) AS title_len
    FROM with_children_plays
    WHERE play_listener_id >= 18
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.play_listener_id
              AND NOT p.is_skipped
        )
      AND s.track_count <= 6
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.release_date,
        s.track_count,
        s.last_track_release_date,
        s.play_id,
        s.play_listener_id,
        s.title_len,
        p.id AS play_id_2,
        p.listener_id AS play_listener_id_2,
        p.is_skipped AS play_is_skipped_2,
        p.ms_played AS play_ms_played
    FROM screened s
    JOIN plays p ON p.track_id = s.id AND p.is_skipped = FALSE
)
SELECT
    id,
    release_date,
    track_count,
    last_track_release_date,
    play_listener_id,
    title_len,
    play_id_2,
    play_listener_id_2,
    play_is_skipped_2,
    play_ms_played,
    LAST_DAY(release_date) AS release_date_month_end
FROM line_level
WHERE title_len >= 6
