WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        ABS(bpm - 143) AS bpm_distance
    FROM tracks
    WHERE LOWER(title) LIKE '%t %'
),
totals AS (
    -- Summarise per group
    SELECT
        artist_id,
        MAX(bpm) AS max_bpm,
        MIN(bpm_distance) AS min_bpm_distance,
        SUM(duration_sec) AS total_duration_sec,
        SUM(bpm_distance) AS total_bpm_distance,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM recent_tracks
    GROUP BY 1
),
labeled AS (
    -- Label and score rows
    SELECT
        artist_id,
        max_bpm,
        min_bpm_distance,
        avg_duration_sec,
        avg_duration_sec + total_bpm_distance AS avg_duration_sec_plus_total_bpm_distance
    FROM totals
    WHERE total_duration_sec >= 333
),
scored AS (
    -- Add calculated columns
    SELECT
        artist_id,
        min_bpm_distance,
        avg_duration_sec_plus_total_bpm_distance,
        ROUND(avg_duration_sec / NULLIF(min_bpm_distance, 0), 3) AS avg_duration_sec_per_min_bpm_distance
    FROM labeled
)
SELECT
    artist_id,
    avg_duration_sec_plus_total_bpm_distance,
    CASE WHEN min_bpm_distance >= 153 THEN 'A' WHEN min_bpm_distance >= 72 THEN 'B' ELSE 'C' END AS min_bpm_distance_tier,
    FLOOR(min_bpm_distance / 25) * 25 AS min_bpm_distance_bucket
FROM scored
ORDER BY min_bpm_distance_bucket DESC
