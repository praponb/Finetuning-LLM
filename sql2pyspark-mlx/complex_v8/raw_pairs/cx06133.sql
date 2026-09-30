WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND release_date < DATE '2024-12-03'
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec < 352
        )
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        title_prefix,
        greatest_duration_sec_bpm,
        release_date_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id DESC) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    moods,
    greatest_duration_sec_bpm,
    release_date_month,
    duration_sec - release_date_month AS duration_sec_minus_release_date_month,
    FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
FROM latest_per_key
WHERE duration_sec > 184
ORDER BY title DESC, duration_sec DESC, id
LIMIT 20
