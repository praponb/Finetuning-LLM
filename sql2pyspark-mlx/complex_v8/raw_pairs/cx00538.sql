WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        FLOOR(bpm / 25) * 25 AS bpm_bucket_2
    FROM tracks
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', title_slug, title) AS title_slug_title_label,
        bpm_bucket - bpm_bucket_2 AS bpm_bucket_minus_bpm_bucket_2
    FROM base
),
unique_rows AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_slug,
        bpm_bucket,
        bpm_bucket_2,
        title_slug_title_label,
        bpm_bucket_minus_bpm_bucket_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm_bucket, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    is_explicit,
    moods,
    title_slug,
    bpm_bucket,
    bpm_bucket_2,
    title_slug_title_label,
    CONCAT_WS('-', title, title_slug_title_label) AS title_title_slug_title_label_label
FROM unique_rows
ORDER BY bpm_bucket, id
LIMIT 10
