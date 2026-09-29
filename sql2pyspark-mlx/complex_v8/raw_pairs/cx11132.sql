WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.release_date_week,
        t.mood_pos,
        t.mood
    FROM valid s
    LATERAL VIEW POSEXPLODE(s.moods) t AS mood_pos, mood
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        bpm,
        release_date_week,
        mood_pos,
        mood,
        bpm + mood_pos AS bpm_plus_mood_pos
    FROM flattened
    WHERE release_date IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT *, QUARTER(release_date_week) AS release_date_week_quarter
    FROM derived
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(bpm - 118) AS bpm_distance,
        LEAST(bpm_plus_mood_pos, bpm) AS least_bpm_plus_mood_pos_bpm,
        YEAR(release_date_week) AS release_date_week_year
    FROM enriched
)
-- Final projection
SELECT
    id,
    mood_pos,
    mood,
    release_date_week_quarter,
    least_bpm_plus_mood_pos_bpm,
    release_date_week_year,
    DATE_FORMAT(release_date_week, 'yyyy-MM-dd HH:00') AS release_date_week_label,
    LOWER(REGEXP_REPLACE(TRIM(mood), ' ', '_')) AS mood_slug
FROM scored
ORDER BY mood NULLS LAST
