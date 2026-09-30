WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        duration_sec + bpm AS duration_sec_plus_bpm,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE duration_sec > 213
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.duration_sec_plus_bpm,
        s.release_date_month_start,
        s.moods_count,
        t.mood
    FROM base s
    LATERAL VIEW OUTER EXPLODE(s.moods) t AS mood
)
-- Subtotals with GROUPING SETS
SELECT
    artist_id,
    mood,
    moods_count,
    GROUPING(artist_id) AS is_total_artist_id,
    ROUND(SUM(moods_count) / NULLIF(SUM(duration_sec_plus_bpm), 0), 3) AS moods_count_to_duration_sec_plus_bpm_ratio,
    ROUND(AVG(bpm), 2) AS avg_bpm
FROM exploded
GROUP BY GROUPING SETS ((artist_id, mood, moods_count), (artist_id))
ORDER BY artist_id NULLS LAST, mood NULLS LAST, moods_count NULLS LAST
