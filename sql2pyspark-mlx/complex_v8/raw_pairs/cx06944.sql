WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE (is_explicit = FALSE OR duration_sec < 402)
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        t.mood
    FROM valid s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        release_date,
        bpm,
        mood,
        DAYOFWEEK(release_date) AS release_date_dayofweek,
        COALESCE(bpm, 0) AS bpm_filled
    FROM per_label
    WHERE mood <> 'focus'
)
SELECT id, mood, field, metric_value
FROM (
    SELECT id, mood, release_date_dayofweek, bpm_filled
    FROM derived
) src
UNPIVOT (
    metric_value FOR field IN (release_date_dayofweek, bpm_filled)
) unp
