WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE (title IS NOT NULL OR bpm > 104)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        COALESCE(duration_sec, 0) AS duration_sec_filled_2,
        LENGTH(TRIM(title)) AS title_len
    FROM cleaned
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        duration_sec_filled,
        duration_sec_filled_2,
        title_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec_filled DESC, id DESC) AS row_num
        FROM annotated
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    duration_sec_filled,
    duration_sec_filled_2
FROM latest_per_key
ORDER BY duration_sec_filled, id
