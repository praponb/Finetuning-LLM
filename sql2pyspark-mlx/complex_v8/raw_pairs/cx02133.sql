WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm
    FROM tracks
    WHERE NOT is_explicit
      AND UPPER(TRIM(title)) LIKE 'P%'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        LENGTH(TRIM(title)) AS title_len,
        LENGTH(TRIM(title)) AS title_len_2
    FROM base
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        bpm,
        title_ref,
        title_len,
        title_len_2,
        LOWER(TRIM(title)) AS title_clean,
        UPPER(TRIM(title_ref)) AS title_ref_clean
    FROM derived
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at
    FROM enriched s
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    bpm,
    title_ref,
    title_len,
    title_clean,
    title_ref_clean,
    play_count,
    first_played_at,
    CONCAT_WS(' / ', title_clean, title_ref_clean) AS title_clean_title_ref_clean_label
FROM with_counts
