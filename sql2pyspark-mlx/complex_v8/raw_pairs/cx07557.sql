WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        UPPER(TRIM(title)) AS title_clean,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref
    FROM tracks
    WHERE bpm < 166
      AND title IS NOT NULL
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(title_ref)) AS title_ref_len,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        CONCAT_WS(' / ', title_clean, title_ref) AS title_clean_title_ref_label
    FROM base
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE duration_sec > (SELECT AVG(duration_sec) FROM derived)
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title_clean,
        s.bpm_per_duration_sec,
        s.title_ref,
        s.release_date_month_start,
        s.title_clean_title_ref_label,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM without_matches s
    JOIN plays p ON p.track_id = s.id
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title_clean,
        s.bpm_per_duration_sec,
        s.title_ref,
        s.play_id,
        s.play_source,
        s.play_listener_id,
        a.genre AS artist_genre
    FROM line_level_plays s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre IN ('HipHop', 'Indie', 'Jazz')
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        title_clean,
        play_id,
        play_listener_id,
        artist_genre,
        LENGTH(TRIM(artist_genre)) AS artist_genre_len,
        CASE WHEN bpm_per_duration_sec >= 7.5 THEN 'A' WHEN bpm_per_duration_sec >= 4.0 THEN 'B' ELSE 'C' END AS bpm_per_duration_sec_tier,
        LENGTH(TRIM(title_ref)) AS title_ref_len
    FROM with_details_artist
    WHERE title_clean IS NOT NULL
)
SELECT
    id,
    title_clean,
    play_id,
    play_listener_id,
    artist_genre,
    bpm_per_duration_sec_tier,
    title_ref_len,
    LENGTH(TRIM(title_clean)) AS title_clean_len,
    COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
FROM scored
WHERE title_ref_len <= 12
ORDER BY title_clean_len, id, play_id
LIMIT 20
