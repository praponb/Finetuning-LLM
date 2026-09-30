WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE (NOT is_explicit OR NOT ARRAY_CONTAINS(moods, 'focus'))
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM scoped_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm < 156
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        CASE WHEN duration_sec >= 312 THEN 'HIGH' WHEN duration_sec >= 154 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier
    FROM screened
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.duration_sec_bucket,
        s.duration_sec_tier,
        a.is_verified AS artist_is_verified
    FROM scored s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with GROUPING SETS
SELECT
    artist_id,
    is_explicit,
    duration_sec_tier,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM enriched
GROUP BY GROUPING SETS ((artist_id, is_explicit, duration_sec_tier), (artist_id))
