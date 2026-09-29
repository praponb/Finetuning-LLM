WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        duration_sec - bpm AS duration_sec_minus_bpm,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
    WHERE SIZE(moods) >= 1
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        SUM(duration_sec) AS total_duration_sec,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        SUM(duration_sec_minus_bpm) AS total_duration_sec_minus_bpm,
        ROUND(AVG(duration_sec_minus_bpm), 2) AS avg_duration_sec_minus_bpm,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list
    FROM recent
    GROUP BY 1
    HAVING AVG(duration_sec) > 142
),
joined AS (
    -- Join artists
    SELECT s.*, a.is_verified AS artist_is_verified, a.artist_name
    FROM per_group s
    LEFT JOIN artists a ON a.id = s.artist_id
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.country AS artist_country, a.is_verified AS artist_is_verified_2
    FROM joined s
    JOIN artists a ON a.id = s.artist_id
),
combined_details AS (
    -- Join artists
    SELECT
        s.artist_id,
        s.total_duration_sec,
        s.total_duration_sec_minus_bpm,
        s.avg_duration_sec_minus_bpm,
        s.distinct_artist_ids,
        s.title_list,
        s.artist_name,
        s.artist_country,
        s.artist_is_verified_2,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified_3
    FROM enriched_artist s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    artist_id,
    total_duration_sec,
    avg_duration_sec_minus_bpm,
    distinct_artist_ids,
    title_list,
    artist_is_verified_2,
    ROUND(total_duration_sec / NULLIF(distinct_artist_ids, 0), 3) AS total_duration_sec_per_distinct_artist_ids,
    ABS(distinct_artist_ids - 7) AS distinct_artist_ids_distance
FROM combined_details
ORDER BY distinct_artist_ids_distance DESC, total_duration_sec NULLS LAST
