WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        bpm + duration_sec AS bpm_plus_duration_sec,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout
    FROM tracks
    WHERE is_explicit = FALSE
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(days_since_release_date / 100) * 100 AS days_since_release_date_bucket
    FROM filtered
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        is_explicit,
        bpm,
        moods,
        days_since_release_date,
        has_workout,
        days_since_release_date_bucket,
        ABS(days_since_release_date - 350) AS days_since_release_date_distance
    FROM tagged
)
-- Final projection
SELECT
    id,
    bpm,
    moods,
    days_since_release_date,
    has_workout,
    days_since_release_date_bucket,
    days_since_release_date_distance
FROM labeled
WHERE ARRAY_CONTAINS(moods, 'workout')
