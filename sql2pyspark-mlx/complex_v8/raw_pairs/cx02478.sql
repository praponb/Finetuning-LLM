WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND LENGTH(TRIM(title)) >= 6
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        title_len,
        artist_id,
        SUM(duration_sec) AS total_duration_sec,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM valid_tracks
    GROUP BY title_len, artist_id
    HAVING AVG(title_len) > 3
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(title_len, distinct_artist_ids) AS greatest_title_len_distinct_artist_ids,
        CASE WHEN distinct_artist_ids >= 6 THEN 'HIGH' WHEN distinct_artist_ids >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_artist_ids_tier
    FROM rollup_base
    WHERE distinct_artist_ids > 2
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date IS NOT NULL
              AND t.is_explicit = TRUE
        )
),
with_parent AS (
    -- Attach artist attributes
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM screened s
    JOIN artists a ON a.id = s.artist_id AND NOT a.is_verified
)
-- Final projection
SELECT
    title_len,
    artist_id,
    total_duration_sec,
    distinct_artist_ids,
    greatest_title_len_distinct_artist_ids,
    distinct_artist_ids_tier,
    artist_is_verified,
    FLOOR(title_len / 2) * 2 AS title_len_bucket
FROM with_parent
ORDER BY artist_id
