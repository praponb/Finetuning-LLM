WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE artist_id <= 2
      AND duration_sec < 312
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        moods_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY moods_count DESC, id DESC) AS row_num
        FROM prepared_tracks
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(moods_count, duration_sec) AS greatest_moods_count_duration_sec
    FROM unique_rows
),
without_matches AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.is_verified = FALSE
              AND a.country = 'JP'
        )
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        moods_count,
        greatest_moods_count_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY greatest_moods_count_duration_sec DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND NOT ARRAY_CONTAINS(t.moods, 'chill')
              AND LOWER(t.title) LIKE '%he%'
        )
)
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    moods,
    moods_count,
    CASE WHEN moods_count >= 2 THEN 'HIGH' WHEN moods_count >= 1 THEN 'MEDIUM' ELSE 'LOW' END AS moods_count_tier,
    ROUND(moods_count / NULLIF(duration_sec, 0), 3) AS moods_count_per_duration_sec
FROM qualified
WHERE duration_sec >= 165
ORDER BY moods_count, artist_id
