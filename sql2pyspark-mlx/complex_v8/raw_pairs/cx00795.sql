WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm,
        ABS(duration_sec - 344) AS duration_sec_distance,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE (artist_id <= 3 OR LENGTH(TRIM(title)) >= 7)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN duration_sec_distance >= 294 THEN 'HIGH' WHEN duration_sec_distance >= 84 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_distance_tier,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        CONCAT(title_prefix, '#', CAST(duration_sec AS STRING)) AS title_prefix_ref
    FROM scoped
    WHERE bpm > 76
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.artist_name
    FROM enriched s
    JOIN artists a ON a.id = s.artist_id
),
enriched_artist_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.least_duration_sec_bpm,
        s.duration_sec_distance,
        s.title_prefix,
        s.duration_sec_distance_tier,
        s.moods_text,
        s.title_prefix_ref,
        s.artist_name,
        a.artist_name AS artist_artist_name,
        a.is_verified AS artist_is_verified
    FROM enriched_artist s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        least_duration_sec_bpm,
        duration_sec_distance,
        title_prefix,
        duration_sec_distance_tier,
        moods_text,
        title_prefix_ref,
        artist_name,
        artist_artist_name,
        artist_is_verified
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS recency_rank
        FROM enriched_artist_2
    ) ranked
    WHERE recency_rank = 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(least_duration_sec_bpm / 50) * 50 AS least_duration_sec_bpm_bucket,
        LEAST(bpm, duration_sec_distance) AS least_bpm_duration_sec_distance,
        COALESCE(duration_sec_distance, 0) AS duration_sec_distance_filled
    FROM unique_rows
)
-- Final projection
SELECT
    id,
    duration_sec,
    bpm,
    moods,
    least_duration_sec_bpm,
    duration_sec_distance,
    title_prefix,
    duration_sec_distance_tier,
    moods_text,
    title_prefix_ref,
    artist_name,
    artist_artist_name,
    artist_is_verified,
    duration_sec_distance_filled
FROM labeled
WHERE NOT artist_is_verified
ORDER BY id DESC, artist_artist_name DESC
