WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        GREATEST(bpm, duration_sec) AS greatest_bpm_duration_sec,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        ABS(duration_sec - 271) AS duration_sec_distance
    FROM tracks
    WHERE NOT is_explicit
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM filtered_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm >= 89
              AND ARRAY_CONTAINS(t.moods, 'party')
        )
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE bpm > (SELECT AVG(bpm) FROM filtered)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        greatest_bpm_duration_sec,
        title_slug,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug_2,
        ROUND(duration_sec_distance / NULLIF(bpm, 0), 3) AS duration_sec_distance_per_bpm,
        FLOOR(duration_sec_distance / 100) * 100 AS duration_sec_distance_bucket
    FROM without_matches
    WHERE SIZE(moods) >= 2
),
artist_id_stats AS (
    -- Max duration_sec per artist_id
    SELECT artist_id, MAX(duration_sec) AS max_duration_sec
    FROM labeled
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_duration_sec,
        ROUND(s.duration_sec - g.max_duration_sec, 2) AS duration_sec_minus_max
    FROM labeled s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec = g.max_duration_sec
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        title_slug_2,
        duration_sec_distance_per_bpm,
        duration_sec_distance_bucket,
        max_duration_sec,
        duration_sec_minus_max,
        CONCAT(title_slug_2, '#', CAST(artist_id AS STRING)) AS title_slug_2_ref
    FROM compared
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    moods,
    duration_sec_distance_per_bpm,
    duration_sec_distance_bucket,
    max_duration_sec,
    duration_sec_minus_max,
    title_slug_2_ref
FROM annotated
ORDER BY duration_sec
