WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        CASE WHEN duration_sec >= 352 THEN 'large' WHEN duration_sec >= 213 THEN 'medium' ELSE 'small' END AS duration_sec_tier,
        CASE WHEN duration_sec >= 344 THEN 'A' WHEN duration_sec >= 183 THEN 'B' ELSE 'C' END AS duration_sec_tier_2
    FROM tracks
    WHERE NOT is_explicit
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.duration_sec_tier,
        s.duration_sec_tier_2,
        t.mood
    FROM recent s
    LATERAL VIEW OUTER EXPLODE(s.moods) t AS mood
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.duration_sec_tier_2,
        s.mood,
        a.genre AS artist_genre
    FROM exploded s
    JOIN artists a ON a.id = s.artist_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT mood, artist_id, duration_sec_tier_2, bpm
    FROM combined_details
) src
PIVOT (
    SUM(bpm)
    FOR duration_sec_tier_2 IN ('B', 'C', 'A')
)
