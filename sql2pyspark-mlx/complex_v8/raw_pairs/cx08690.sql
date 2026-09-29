WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout
    FROM tracks
    WHERE is_explicit = TRUE
      AND bpm BETWEEN 76 AND 170
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(bpm / NULLIF(bpm_bucket, 0), 3) AS bpm_per_bpm_bucket,
        YEAR(release_date) AS release_date_year,
        FLOOR(bpm / 25) * 25 AS bpm_bucket_2
    FROM filtered_tracks
),
annotated AS (
    -- Add calculated columns
    SELECT *, DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM labeled
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.has_workout,
        s.bpm_bucket_2,
        s.release_date_dayofweek,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.source AS play_source
    FROM annotated s
    LEFT JOIN plays p ON p.track_id = s.id
)
SELECT id, play_id, metric_name, metric_value
FROM (
    SELECT id, play_id, release_date_dayofweek, bpm_bucket_2
    FROM with_children_plays
) src
UNPIVOT (
    metric_value FOR metric_name IN (release_date_dayofweek, bpm_bucket_2)
) unp
