WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        DATE_ADD(release_date, 30) AS release_date_plus_30d,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE is_explicit = TRUE
      AND title IS NOT NULL
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        release_date_plus_30d,
        release_date_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id DESC) AS recency_rank
        FROM prepared
    ) ranked
    WHERE recency_rank = 1
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.artist_name
    FROM unique_rows s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    release_date_plus_30d,
    release_date_label,
    artist_is_verified,
    artist_country,
    artist_name,
    CONCAT_WS('|', release_date_label, artist_country) AS release_date_label_artist_country_label
FROM with_details_artist
ORDER BY artist_name NULLS LAST, id
LIMIT 10
