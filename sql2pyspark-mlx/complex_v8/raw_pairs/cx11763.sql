WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods
    FROM tracks
    WHERE (NOT ARRAY_CONTAINS(moods, 'focus') OR bpm < 155)
),
enriched AS (
    -- Derive helper columns
    SELECT *, ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM scoped
    WHERE artist_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        UPPER(TRIM(title)) AS title_clean
    FROM enriched
    WHERE duration_sec > 184
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.duration_sec_per_bpm,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM scored s
    JOIN artists a ON a.id = s.artist_id
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.duration_sec_per_bpm,
        s.artist_country,
        s.artist_is_verified,
        t.mood
    FROM with_details_artist s
    LATERAL VIEW OUTER EXPLODE(s.moods) t AS mood
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT mood, artist_country, duration_sec_per_bpm
    FROM flattened
) src
PIVOT (
    AVG(duration_sec_per_bpm)
    FOR artist_country IN ('KR', 'JP', 'TH')
)
