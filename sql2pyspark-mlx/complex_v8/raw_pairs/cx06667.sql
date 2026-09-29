WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(duration_sec - 279) AS duration_sec_distance
    FROM tracks
    WHERE duration_sec > 164
),
summary AS (
    -- Summarise per group
    SELECT
        is_explicit,
        ROUND(SUM(duration_sec_distance) / NULLIF(SUM(duration_sec), 0), 3) AS duration_sec_distance_to_duration_sec_ratio,
        MIN(duration_sec) AS min_duration_sec
    FROM filtered
    WHERE release_date < DATE '2025-08-07'
    GROUP BY 1
),
filtered_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE min_duration_sec > (SELECT MIN(min_duration_sec) FROM summary)
)
-- Final projection
SELECT
    is_explicit,
    duration_sec_distance_to_duration_sec_ratio,
    ABS(min_duration_sec - 238) AS min_duration_sec_distance
FROM filtered_2
WHERE duration_sec_distance_to_duration_sec_ratio >= 2.5
ORDER BY min_duration_sec_distance DESC, duration_sec_distance_to_duration_sec_ratio
