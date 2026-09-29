WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        CASE WHEN debut_year >= 2018 THEN 'large' WHEN debut_year >= 1997 THEN 'medium' ELSE 'small' END AS debut_year_tier
    FROM artists
    WHERE UPPER(TRIM(artist_name)) LIKE 'L%'
      AND is_verified = TRUE
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug,
        CONCAT(artist_name, '#', CAST(debut_year AS STRING)) AS artist_name_ref
    FROM scoped
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        debut_year,
        country_slug,
        artist_name_ref,
        LOWER(REGEXP_REPLACE(TRIM(debut_year_tier), ' ', '_')) AS debut_year_tier_slug
    FROM enriched
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MIN(duration_sec) AS min_track_duration_sec,
        COUNT(*) AS track_count,
        COUNT(*) AS track_count_2
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.min_track_duration_sec,
        COALESCE(t.track_count, 0) AS track_count,
        COALESCE(t.track_count_2, 0) AS track_count_2
    FROM labeled s
    LEFT JOIN track_stats t ON t.artist_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    track_count,
    track_count_2,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(debut_year, 0.5) AS median_debut_year
FROM with_counts
GROUP BY GROUPING SETS ((track_count, track_count_2), (track_count), (track_count_2))
ORDER BY track_count NULLS LAST, track_count_2 NULLS LAST
