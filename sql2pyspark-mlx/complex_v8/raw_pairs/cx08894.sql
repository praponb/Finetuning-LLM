WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        COALESCE(bpm, 0) AS bpm_filled,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE is_explicit = TRUE
      AND title IS NOT NULL
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(' / ', moods_text, title_ref) AS moods_text_title_ref_label
    FROM valid
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        bpm_filled,
        moods_text,
        title_ref,
        moods_text_title_ref_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS rn
        FROM tagged
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    bpm,
    bpm_filled,
    moods_text,
    ABS(bpm_filled - 102) AS bpm_filled_distance
FROM latest
WHERE bpm_filled > 108
ORDER BY id
