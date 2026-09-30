WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
    WHERE duration_sec >= 154
      AND LENGTH(TRIM(title)) >= 6
),
with_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.release_date_label,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year > 1997
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.release_date_label,
        s.artist_country,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped
    FROM with_details s
    JOIN plays p ON p.track_id = s.id
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN duration_sec >= 352 THEN 'HIGH' WHEN duration_sec >= 164 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier
    FROM detail_rows
    WHERE artist_id >= 9
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    release_date_label,
    artist_country,
    play_id,
    play_listener_id,
    play_is_skipped,
    duration_sec_tier,
    DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label_2,
    IF(play_is_skipped, 'yes', 'no') AS play_is_skipped_label
FROM annotated
ORDER BY release_date_label
