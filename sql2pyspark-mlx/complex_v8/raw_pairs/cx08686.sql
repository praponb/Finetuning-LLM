WITH base AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        artist_id,
        is_explicit,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(bpm) AS total_bpm
    FROM latest
    WHERE is_explicit = TRUE
    GROUP BY artist_id, is_explicit, DATE_FORMAT(release_date, 'yyyy-MM')
    HAVING MAX(duration_sec) > 209
)
-- Final projection
SELECT
    artist_id,
    is_explicit,
    release_date_ym,
    distinct_ids,
    total_bpm
FROM grouped
ORDER BY release_date_ym, artist_id, is_explicit
LIMIT 10
