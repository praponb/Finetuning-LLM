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
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE NOT is_explicit
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.release_date_year,
        t.mood_pos,
        t.mood
    FROM filtered_tracks s
    LATERAL VIEW OUTER POSEXPLODE(s.moods) t AS mood_pos, mood
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.artist_name
    FROM flattened s
    JOIN artists a ON a.id = s.artist_id AND a.genre IS NOT NULL
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        release_date_year,
        mood_pos,
        mood,
        artist_country,
        artist_name,
        mood_pos - bpm AS mood_pos_minus_bpm,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, mood_pos, measure, reading
FROM (
    SELECT id, mood_pos, mood_pos_minus_bpm, duration_sec_bucket
    FROM enriched_2
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (mood_pos_minus_bpm, duration_sec_bucket)
) unp
ORDER BY id, mood_pos, measure
